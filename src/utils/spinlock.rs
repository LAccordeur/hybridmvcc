use std::{
    cell::UnsafeCell,
    marker::Sync,
    ops::{Deref, DerefMut, Drop},
    sync::atomic::{AtomicBool, Ordering},
};

pub struct Spinlock<T> {
    lock: AtomicBool,
    data: UnsafeCell<T>,
}

pub struct SpinlockGuard<'a, T: 'a> {
    lock: &'a AtomicBool,
    data: &'a mut T,
}

unsafe impl<T> Sync for Spinlock<T> {}

impl<T> Spinlock<T> {
    pub fn new(data: T) -> Self {
        Self {
            lock: AtomicBool::new(false),
            data: UnsafeCell::new(data),
        }
    }

    pub fn lock(&self) -> SpinlockGuard<T> {
        while self.lock.compare_and_swap(false, true, Ordering::AcqRel) {}

        SpinlockGuard {
            lock: &self.lock,
            data: unsafe { &mut *self.data.get() },
        }
    }
}

impl<'a, T> Drop for SpinlockGuard<'a, T> {
    fn drop(&mut self) {
        self.lock.store(false, Ordering::Release);
    }
}

impl<'a, T> Deref for SpinlockGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        &*self.data
    }
}

impl<'a, T> DerefMut for SpinlockGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut *self.data
    }
}
