pub fn save_many<I, T, O, E>(mut inputs: I, saver: O) -> Result<u64, E>
where
    I: Iterator<Item = T>,
    O: Fn(T) -> Result<u64, E>,
{
    inputs.try_fold(0, |cnt, input| saver(input).map(|c| c + cnt))
}
