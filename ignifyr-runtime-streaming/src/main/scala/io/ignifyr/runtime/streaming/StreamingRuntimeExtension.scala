package io.ignifyr.runtime.streaming

import io.ignifyr.engine.spi.{IgnifyrExtension, StreamingExecutionProvider}

/**
 * Registers the streaming execution capability with the engine via ServiceLoader.
 */
class StreamingRuntimeExtension extends IgnifyrExtension {

  override val id: String = "runtime-streaming"

  override def streamingProvider: Option[StreamingExecutionProvider] = Some(new StreamingJobExecutor)
}
