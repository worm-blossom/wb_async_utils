//! Provides [`SharedConsumer`], a way for creating multiple independent handles that coordinate termporary exclusive access to a shared underlying consumer.

use core::{
    cell::Cell,
    fmt::Debug,
    ops::{Deref, DerefMut},
};
use std::rc::Rc;

use ufotofu::{BufferedConsumer, BulkConsumer, Consumer};

use crate::{mutex::WriteGuard, Mutex};

/// The state shared between all clones of the same [`SharedConsumer`].
#[derive(Debug)]
struct State<C, ConsumerErr> {
    m: Mutex<MutexState<C, ConsumerErr>>,
    unclosed_handle_count: Cell<usize>,
}

impl<C, ConsumerErr> State<C, ConsumerErr> {
    /// Creates a new [`State`] for managing shared access to the same `consumer`.
    fn new(consumer: C) -> Self {
        State {
            m: Mutex::new(MutexState {
                c: consumer,
                error: None,
            }),
            unclosed_handle_count: Cell::new(1),
        }
    }
}

#[derive(Debug)]
struct MutexState<C, ConsumerErr> {
    c: C,
    error: Option<ConsumerErr>,
}

/// A consumer adaptor that allows access to the same consumer from multiple parts in the codebase by providing a cloneable handle.
///
/// This type provides three core pieces of functionality: ensuring exclusive access to the underlying consumer so that independent components do not interleave each other's data, ignoring all `close`s except for the very last one, and cloning and caching errors to present them to all handles. More specifically:
///
/// A [`SharedConsumer`] handle does not itself implement the consumer traits. Instead, one must call the async [`access_consumer`](SharedConsumer::access_consumer) method, which grants a [`SharedConsumerAccess`] which implements the consumer traits. If another [`SharedConsumerAccess`] is currently alive, the method non-blocks until the inner consumer can be accessed safely. Pending accesses are woken up in FIFO-order.
///
/// Calling `close` on any handle is a no-op that reports a success and drops the supplied final value, except when there exists exactly one handle which hasn't been closed yet. In that case, the inner consumer is closed with the supplied final value. If you create a [`SharedConsumer`] but then never call [`access_consumer`](SharedConsumer::access_consumer) to `close` the returned [`SharedConsumerAccess`], then the underlying consumer will never be closed.
///
/// The `Error` type of the inner consumer must implement [`Clone`]. Once the inner consumer emits an error, all [`SharedConsumerAccess`] handles will emit clones of that value on all operations. The implementation ensures that the inner consumer is not used after an error.
///
/// ```
/// use core::time::Duration;
/// use either::Either::*;
/// use wb_async_utils::shared_consumer::*;
/// use smol::{Timer, block_on};
/// use ufotofu::{Consumer, consumer::{TestConsumer, TestConsumerBuilder}};
///
/// let underlying_c: TestConsumer<u8, (), i16> = TestConsumerBuilder::new(-4, 3).build();
///
/// let shared1 = SharedConsumer::new(underlying_c);
/// let shared2 = shared1.clone();
///
/// let write_some_items1 = async {
///     {
///         let mut c_handle = shared1.access_consumer().await;
///         Timer::after(Duration::from_millis(50)).await; // Since we hold a handle right now, obtaining the second handle has to wait for us.
///         assert_eq!(Ok(()), c_handle.consume(1).await);
///     }
///
///     Timer::after(Duration::from_millis(50)).await; // Having dropped p_handle, the other task can jump in now.
///
///     {
///         let mut c_handle = shared1.access_consumer().await;
///         assert_eq!(Ok(()), c_handle.consume(3).await);
///         assert_eq!(Err(-4), c_handle.consume(4).await);
///     }
/// };
///
/// let write_some_items2 = async {
///     Timer::after(Duration::from_millis(10)).await; // Ensure that the other task "starts".
///
///     {
///         let mut c_handle = shared2.access_consumer().await;
///         assert_eq!(Ok(()), c_handle.consume(2).await);
///     }
///
///     Timer::after(Duration::from_millis(50)).await;
///
///     let mut c_handle = shared2.access_consumer().await;
///     assert_eq!(Err(-4), c_handle.consume(4).await); // Replays a cached `-4` instead of using the underlying consumer.
/// };
///
/// block_on(futures::future::join(write_some_items1, write_some_items2));
/// ```
#[derive(Debug)]
pub struct SharedConsumer<C, ConsumerErr>
where
    C: Consumer,
{
    state_ref: Rc<State<C, ConsumerErr>>,
}

impl<C, ConsumerErr> Clone for SharedConsumer<C, ConsumerErr>
where
    C: Consumer,
{
    fn clone(&self) -> Self {
        self.state_ref
            .deref()
            .unclosed_handle_count
            .set(self.state_ref.deref().unclosed_handle_count.get() + 1);

        Self {
            state_ref: self.state_ref.clone(),
        }
    }
}

impl<C, ConsumerErr> SharedConsumer<C, ConsumerErr>
where
    C: Consumer,
{
    /// Creates a new `SharedConsumer` from a cloneable reference to a [`State`].
    pub fn new(c: C) -> Self {
        Self {
            state_ref: Rc::new(State::new(c)),
        }
    }

    /// Obtains exclusive access to the underlying consumer, waiting if necessary.
    pub async fn access_consumer(&self) -> SharedConsumerAccess<C, ConsumerErr> {
        SharedConsumerAccess {
            c: self.state_ref.deref().m.write().await,
            unclosed_handle_count: &self.state_ref.deref().unclosed_handle_count,
        }
    }
}

/// A handle that represents exclusive access to an underlying shared consumer. Implements the consumer traits and forwards method calls to the underlying consumer. After the underlying consumer has emitted its error, a [`SharedConsumerAccess`] replays copies of that error instead of continuing to call methods on the underlying consumer.
#[derive(Debug)]
pub struct SharedConsumerAccess<'shared_consumer, C, ConsumerErr> {
    c: WriteGuard<'shared_consumer, MutexState<C, ConsumerErr>>,
    unclosed_handle_count: &'shared_consumer Cell<usize>,
}

impl<C, ConsumerErr> Consumer for SharedConsumerAccess<'_, C, ConsumerErr>
where
    C: Consumer<Error = ConsumerErr>,
    C::Final: Clone,
    ConsumerErr: Clone,
{
    type Item = C::Item;

    type Final = C::Final;

    type Error = C::Error;

    async fn consume(&mut self, item: Self::Item) -> Result<(), Self::Error> {
        let inner_state = self.c.deref_mut();

        match inner_state.error.as_ref() {
            Some(err) => Err(err.clone()),
            None => match inner_state.c.consume(item).await {
                Ok(()) => Ok(()),
                Err(err) => {
                    inner_state.error = Some(err.clone());
                    Err(err)
                }
            },
        }
    }

    async fn close(&mut self, fin: Self::Final) -> Result<(), Self::Error> {
        let inner_state = self.c.deref_mut();

        match inner_state.error.as_ref() {
            Some(err) => Err(err.clone()),
            None => {
                self.unclosed_handle_count
                    .set(self.unclosed_handle_count.get() - 1);

                if self.unclosed_handle_count.get() == 0 {
                    // Closing the final handle.
                    match inner_state.c.close(fin).await {
                        Ok(()) => Ok(()),
                        Err(err) => {
                            inner_state.error = Some(err.clone());
                            Err(err)
                        }
                    }
                } else {
                    // Not the final handle to be closed, so do nothing.
                    Ok(())
                }
            }
        }
    }
}

impl<C, ConsumerErr> BufferedConsumer for SharedConsumerAccess<'_, C, ConsumerErr>
where
    C: BufferedConsumer<Error = ConsumerErr>,
    C::Final: Clone,
    ConsumerErr: Clone,
{
    async fn flush(&mut self) -> Result<(), Self::Error> {
        let inner_state = self.c.deref_mut();

        match inner_state.error.as_ref() {
            Some(err) => Err(err.clone()),
            None => match inner_state.c.flush().await {
                Ok(()) => Ok(()),
                Err(err) => {
                    inner_state.error = Some(err.clone());
                    Err(err)
                }
            },
        }
    }
}

impl<C, ConsumerErr> BulkConsumer for SharedConsumerAccess<'_, C, ConsumerErr>
where
    C: BulkConsumer<Error = ConsumerErr>,
    C::Final: Clone,
    ConsumerErr: Clone,
{
    async fn expose_slots<'a>(&'a mut self) -> Result<&'a mut [Self::Item], Self::Error>
    where
        Self::Item: 'a,
    {
        let inner_state = self.c.deref_mut();

        match inner_state.error.as_ref() {
            Some(err) => Err(err.clone()),
            None => match inner_state.c.expose_slots().await {
                Ok(slots) => Ok(slots),
                Err(err) => {
                    inner_state.error = Some(err.clone());
                    Err(err)
                }
            },
        }
    }

    async fn consume_slots(&mut self, amount: usize) -> Result<(), Self::Error> {
        let inner_state = self.c.deref_mut();

        match inner_state.error.as_ref() {
            Some(err) => Err(err.clone()),
            None => match inner_state.c.consume_slots(amount).await {
                Ok(()) => Ok(()),
                Err(err) => {
                    inner_state.error = Some(err.clone());
                    Err(err)
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::time::Duration;
    use either::Either::{Left, Right};
    use smol::{block_on, Timer};
    use ufotofu::{
        consumer::{TestConsumer, TestConsumerBuilder},
        Consumer, Producer,
    };
    use ufotofu_queues::Fixed;

    use crate::spsc::{self, new_spsc};

    #[test]
    fn test_shared_consumer_errors() {
        let underlying_c: TestConsumer<u8, (), i16> = TestConsumerBuilder::new(-4, 3).build();

        let shared1 = SharedConsumer::new(underlying_c);
        let shared2 = shared1.clone();

        let write_some_items1 = async {
            {
                let mut c_handle = shared1.access_consumer().await;
                Timer::after(Duration::from_millis(50)).await; // Since we hold a handle right now, obtaining the second handle has to wait for us.
                assert_eq!(Ok(()), c_handle.consume(1).await);
            }

            Timer::after(Duration::from_millis(50)).await; // Having dropped p_handle, the other task can jump in now.

            {
                let mut c_handle = shared1.access_consumer().await;
                assert_eq!(Ok(()), c_handle.consume(3).await);
                assert_eq!(Err(-4), c_handle.consume(4).await);
            }
        };

        let write_some_items2 = async {
            Timer::after(Duration::from_millis(10)).await; // Ensure that the other task "starts".

            {
                let mut c_handle = shared2.access_consumer().await;
                assert_eq!(Ok(()), c_handle.consume(2).await);
            }

            Timer::after(Duration::from_millis(50)).await;

            let mut c_handle = shared2.access_consumer().await;
            assert_eq!(Err(-4), c_handle.consume(4).await); // Replays a cached `-4` instead of using the underlying consumer.
        };

        block_on(futures::future::join(write_some_items1, write_some_items2));
    }

    #[test]
    fn test_shared_consumer_closing() {
        let spsc_state: spsc::State<Fixed<u8>, i16, ()> =
            spsc::State::new(Fixed::new(16 /* capacity */));
        let (sender, mut receiver) = new_spsc(&spsc_state);

        let shared1 = SharedConsumer::new(sender);
        let shared2 = shared1.clone();

        let write_some_items1 = async {
            {
                let mut c_handle = shared1.access_consumer().await;
                Timer::after(Duration::from_millis(50)).await; // Since we hold a handle right now, obtaining the second handle has to wait for us.
                assert_eq!(Ok(()), c_handle.consume(1).await);
            }

            Timer::after(Duration::from_millis(50)).await; // Having dropped p_handle, the other task can jump in now.

            {
                let mut c_handle = shared1.access_consumer().await;
                assert_eq!(Ok(()), c_handle.consume(3).await);
                assert_eq!(Ok(()), c_handle.close(-1).await);
            }
        };

        let write_some_items2 = async {
            Timer::after(Duration::from_millis(10)).await; // Ensure that the other task "starts".

            {
                let mut c_handle = shared2.access_consumer().await;
                assert_eq!(Ok(()), c_handle.consume(2).await);
            }

            Timer::after(Duration::from_millis(50)).await;

            let mut c_handle = shared2.access_consumer().await;
            assert_eq!(Ok(()), c_handle.close(-2).await); // Replays a cached `-4` instead of using the underlying consumer.

            assert_eq!(Ok(Left(1)), receiver.produce().await);
            assert_eq!(Ok(Left(2)), receiver.produce().await);
            assert_eq!(Ok(Left(3)), receiver.produce().await);
            assert_eq!(Ok(Right(-2)), receiver.produce().await);
        };

        block_on(futures::future::join(write_some_items1, write_some_items2));
    }
}
