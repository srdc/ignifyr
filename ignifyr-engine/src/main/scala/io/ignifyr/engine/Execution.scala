package io.ignifyr.engine

import akka.actor.ActorSystem

object Execution {
  implicit lazy val actorSystem: ActorSystem = ActorSystem("Ignifyr")
}
