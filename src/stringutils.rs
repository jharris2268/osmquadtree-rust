pub trait StringUtils {
    fn substr(&self, start: usize, len: usize) -> Self;
}

impl StringUtils for String {
    fn substr(&self, start: usize, len: usize) -> Self {
        self.chars().skip(start).take(len).collect()
    }
}

