# frozen_string_literal: true

module AdvancedSneakersActiveJob
  class PublishError < StandardError; end

  # Raised when a delay exceeds LeveledDelayedPublisher::MAX_DELAY.
  class DelayTooLargeError < PublishError; end

  # Raised by LeveledDelayedPublisher#declare_topology! when the broker is too
  # old to support quorum-queue message TTL (RabbitMQ < 3.10).
  class BrokerVersionError < StandardError; end
end
