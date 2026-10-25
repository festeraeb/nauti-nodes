//! Remote inference API (NVIDIA NIM over the internet): ordered model
//! fallback. `NimRuntime` (src/adapter/nim.rs) returns errors such as
//! "nim inference returned status 404 Not Found"; these helpers read the
//! status from that text and move on to the next model when it makes sense.

use crate::runtime::{ChatMessage, InferenceResponse, ModelRuntime};

/// The HTTP status code in a runtime error string: the 3-digit number right
/// after the word "status " (e.g. "... returned status 503 Service
/// Unavailable" -> Some(503)). None when there is none.
pub fn status_in_error(err: &str) -> Option<u16> {
    let _ = err;
    todo!("NN1c: implement status_in_error")
}

/// True when the error means "try the next model": status 404, 410, 429 or
/// any 500-599. False for every other status and for errors with no status.
pub fn should_try_next_model(err: &str) -> bool {
    let _ = err;
    todo!("NN1c: implement should_try_next_model")
}

/// Call `runtime.infer` with each id in `models`, in order. Return the first
/// success with its `model` field set to the id that answered. If an error
/// is not `should_try_next_model`, return it at once without trying more
/// models. If every model fails, return the last error. Empty `models`
/// gives Err("no remote models configured").
pub async fn infer_with_fallback(
    runtime: &dyn ModelRuntime,
    models: &[String],
    messages: Vec<ChatMessage>,
    max_tokens: Option<u32>,
) -> Result<InferenceResponse, String> {
    let _ = (runtime, models, messages, max_tokens);
    todo!("NN1c: implement infer_with_fallback")
}
