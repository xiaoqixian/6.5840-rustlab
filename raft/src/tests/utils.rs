// Date:   Thu Oct 17 21:55:33 2024
// Mail:   lunar_ubuntu@qq.com
// Author: https://github.com/xiaoqixian

pub struct ModNum<N, M> {
    num: N,
    md: M,
}

impl<N, M, R, O> std::ops::Add<R> for ModNum<N, M>
where N: std::ops::Add<R, Output = O>, 
      O: std::ops::Rem<M, Output = N>,
      M: Clone
{
    type Output = Self;
    fn add(self, rhs: R) -> Self::Output {
        let Self { num, md } = self;
        Self {
            num: (num + rhs) % md.clone(),
            md
        }
    }
}

use rand::Rng;

pub fn gen_bool(possibility: f64) -> bool {
    rand::thread_rng().gen_bool(possibility)
}

pub fn randu32() -> u32 {
    rand::thread_rng().gen()
}

pub fn randu64() -> u64 {
    rand::thread_rng().gen()
}

pub fn randusize() -> usize {
    rand::thread_rng().gen()
}
