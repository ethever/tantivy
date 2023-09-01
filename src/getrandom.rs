/// A getrandom implementation that always fails
pub fn task(buf: &mut [u8]) -> std::result::Result<(), getrandom::Error> {
    let time = ic_cdk::api::time();
    let bytes = time.to_le_bytes();
    buf.copy_from_slice(&[bytes, bytes, bytes, bytes].concat());
    Ok(())
}

getrandom::register_custom_getrandom!(task);
