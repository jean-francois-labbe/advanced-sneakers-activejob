# frozen_string_literal: true

module AdvancedSneakersActiveJob
  class PublishError < StandardError; end

  # Raised when a delay exceeds LeveledDelayedPublisher::MAX_DELAY.
  class DelayTooLargeError < PublishError; end
end
