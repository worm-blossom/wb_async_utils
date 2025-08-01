//! This module provides an abstraction for two communicating endpoints: a synchronous `Sender` sends data to an async `Receiver`. When the `Sender` is instructed to send even though the `Receiver` has not yet accepted the previously sent item, that item is simply overwritten and lost forever. The `Sender` can further indicate that it will not send any more data in the future (this maps to the `Producer` implementation of the `Receiver` emitting a `Final` item).

use either::Either::{self, *};
use std::{convert::Infallible, marker::PhantomData, rc::Rc};
use ufotofu::Producer;

use crate::TakeCell;

/// The state shared between the [`Sender`] and the [`Receiver`].
#[derive(Debug, Default)]
struct State<Item, Final> {
    cell: TakeCell<Either<Item, Final>>,
}

impl<Item, Final> State<Item, Final> {
    /// Creates a new [`State`]. Nothing more to say, state is fully opaque.
    fn new() -> Self {
        State {
            cell: TakeCell::new(),
        }
    }
}

/// Creates a new Shelf in the form of a [`Sender`] and a [`Receiver`] endpoint which communicate via the given [`State`].
///
/// An example with a stack-allocated [`State`]:
///
/// ```
/// use wb_async_utils::shelf::*;
/// use core::time::Duration;
/// use ufotofu::Producer;
/// use smol::{block_on, Timer};
///
/// block_on(async {
///     let (mut sender, mut receiver) = new_shelf();
///
///     let send_things = async {
///         sender.set(0);
///         Timer::after(Duration::from_millis(50)).await;
///         sender.set(1); // will never be read
///         sender.set(2);
///         Timer::after(Duration::from_millis(50)).await;
///         sender.close(-17);
///     };
///
///     let receive_things = async {
///         assert_eq!(0, receiver.produce().await.unwrap().unwrap_left());
///         assert_eq!(2, receiver.produce().await.unwrap().unwrap_left());
///         assert_eq!(-17, receiver.produce().await.unwrap().unwrap_right());
///     };
///
///     futures::join!(send_things, receive_things);
/// });
/// ```
pub fn new_shelf<Item, Final>() -> (Sender<Item, Final>, Receiver<Item, Final>) {
    let state = Rc::new(State::new());
    (
        Sender {
            state: state.clone(),
            phantom: PhantomData,
        },
        Receiver {
            state: state,
            phantom: PhantomData,
        },
    )
}

/// Allows sending data to the corresponding [`Receiver`], and to indicate that no more data will follow.
#[derive(Debug)]
pub struct Sender<Item, Final> {
    state: Rc<State<Item, Final>>,
    phantom: PhantomData<(Item, Final)>,
}

impl<Item, Final> Sender<Item, Final> {
    /// Sets the shelf to a new value. There (deliberately) is no way to detect whether the previous value had been received by the corresponding `Receiver` or not.
    ///
    /// Must not call this after having called `close`.
    pub fn set(&self, item: Item) {
        self.state.cell.set(Left(item))
    }

    /// Indicates to the corresponding [`Sender`] that no more items will be set. Must not be called multiple times.
    pub fn close(&mut self, fin: Final) {
        self.state.cell.set(Right(fin))
    }

    /// Updates the shelf's current value (if any) with a synchronous function.
    pub fn update(&self, with: impl FnOnce(Option<Either<Item, Final>>) -> Either<Item, Final>) {
        self.state.cell.update(with)
    }
}

/// Allows receiving data from the corresponding [`Sender`] via a [`Producer`] implementation.
#[derive(Debug)]
pub struct Receiver<Item, Final> {
    state: Rc<State<Item, Final>>,
    phantom: PhantomData<(Item, Final)>,
}

impl<Item, Final> Receiver<Item, Final> {
    /// Returns whether there is currently no value set.
    pub fn is_empty(&self) -> bool {
        self.state.cell.is_empty()
    }
}

impl<Item, Final> Producer for Receiver<Item, Final> {
    type Item = Item;

    type Final = Final;

    type Error = Infallible;

    /// Take an item from the buffer queue, waiting for an item to
    /// become available (by being consumed by the corresponding [`Sender`]) if necessary.
    async fn produce(&mut self) -> Result<Either<Self::Item, Self::Final>, Self::Error> {
        Ok(self.state.cell.take().await)
    }
}
