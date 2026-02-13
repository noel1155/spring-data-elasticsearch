package org.springframework.data.elasticsearch.core;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

/**
 * Unit test to verify the behavior of Reactor operators used in the refactored
 * save method.
 * This ensures that error propagation works as expected.
 */
public class ReactorBehaviorUnitTest {

        @Test
        void shouldPropagateErrorsAfterEmittingBufferedItems() {
                Flux<String> flux = Flux.just("one", "two")
                                .concatWith(Flux.error(new RuntimeException("test error")))
                                .bufferTimeout(1, Duration.ofMillis(200), true)
                                .concatMapDelayError(list -> Flux.fromIterable(list));

                StepVerifier.create(flux)
                                .expectNext("one", "two")
                                .expectErrorMatches(throwable -> throwable instanceof RuntimeException
                                                && throwable.getMessage().equals("test error"))
                                .verify();
        }

        @Test
        void shouldHandleMultipleBatchesBeforeError() {
                Flux<String> flux = Flux.just("one", "two", "three", "four")
                                .concatWith(Flux.error(new RuntimeException("test error")))
                                .bufferTimeout(2, Duration.ofMillis(200), true)
                                .concatMapDelayError(list -> Flux.fromIterable(list));

                StepVerifier.create(flux)
                                .expectNext("one", "two", "three", "four")
                                .expectErrorMatches(throwable -> throwable instanceof RuntimeException
                                                && throwable.getMessage().equals("test error"))
                                .verify();
        }

        @Test
        void shouldEmitPartialBufferBeforeError() {
                // This matches the integration test: bulkSize=10 but only 2 entities before
                // error
                Flux<String> flux = Flux.just("one", "two")
                                .concatWith(Flux.error(new RuntimeException("test error")))
                                .bufferTimeout(10, Duration.ofMillis(200), true)
                                .concatMapDelayError(list -> Flux.fromIterable(list));

                StepVerifier.create(flux)
                                .expectNext("one", "two")
                                .expectErrorMatches(throwable -> throwable instanceof RuntimeException
                                                && throwable.getMessage().equals("test error"))
                                .verify();
        }

        @Test
        void shouldEmitPartialBufferBeforeErrorWithAsyncProcessing() {
                // Simulates real saveAll: async processing with delay (like network call to ES)
                Flux<String> flux = Flux.just("one", "two")
                                .concatWith(Flux.error(new RuntimeException("test error")))
                                .bufferTimeout(10, Duration.ofMillis(200), true)
                                .concatMapDelayError(
                                                list -> Flux.fromIterable(list).delayElements(Duration.ofMillis(50)));

                StepVerifier.create(flux)
                                .expectNext("one", "two")
                                .expectErrorMatches(throwable -> throwable instanceof RuntimeException
                                                && throwable.getMessage().equals("test error"))
                                .verify(Duration.ofSeconds(5));
        }
}
