use std::boxed::Box;
use std::iter::Iterator;
use std::ops::DerefMut;

use super::error::ExecutorError;
use super::operator::PhysicalOperator;
use super::schema::*;

pub struct PhysicalPlan<'a> {
    pub root: Box<dyn PhysicalOperator + 'a>,
}

impl PhysicalPlan<'_> {
    pub fn open(&mut self) -> Result<(), ExecutorError> {
        self.root.deref_mut().open()
    }
}

impl Iterator for PhysicalPlan<'_> {
    type Item = Message;

    fn next(&mut self) -> Option<Self::Item> {
        self.root.deref_mut().next()
    }
}

impl<'a> From<Box<dyn PhysicalOperator + 'a>> for PhysicalPlan<'a> {
    fn from(value: Box<dyn PhysicalOperator + 'a>) -> Self {
        Self { root: value }
    }
}
