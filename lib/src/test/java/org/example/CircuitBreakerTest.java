package org.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType;
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerEvent;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.decorators.Decorators;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;

public class CircuitBreakerTest {
    private static final Logger log = LoggerFactory.getLogger(CircuitBreakerTest.class);

    @Test
    void circuitBreakerAbleToSwitchToFallback() throws InterruptedException {
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofMillis(1))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(3)
                .slidingWindowType(SlidingWindowType.COUNT_BASED)
                .build();

        CircuitBreaker circuitBreaker = CircuitBreaker.of("cb", circuitBreakerConfig);
        var evtPub = circuitBreaker.getEventPublisher();
        evtPub.onEvent((CircuitBreakerEvent evt) -> {
            log.info(evt.toString());
        });
        // Let's make an executor service that has queue with limited capacity and which
        // throws exceptions when trying to submit a task in case its queue is already
        // full. We dont specify rejection handler, so default is used - AbortPolicy,
        // throwing RejectedExecutionException if there's no space in the queue
        var boundedExecSvc = new ThreadPoolExecutor(4, 4, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(3));
        var unboundedExecSvc = Executors.newScheduledThreadPool(4);
        var futs = new ArrayList<CompletableFuture<String>>();
        int numRqs = 100;
        int numErrors = 0;

        IntervalFunction retryIntervalFn = IntervalFunction.ofExponentialRandomBackoff(1000, 2.0, 0.5);
        var retryConfig = RetryConfig.custom()
                .intervalFunction(retryIntervalFn)
                .maxAttempts(5)
                .build();
        var retry = Retry.of("rtr", retryConfig);
        retry.getEventPublisher().onEvent(evt -> log.info(evt.toString()));

        for (int i = 0; i < numRqs; i++) {
            final int localI = i;
            try {
                Supplier<CompletionStage<String>> nonResilientFunc = () -> {
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            Thread.sleep(1000);
                            return "Result " + localI;
                        } catch (InterruptedException e) {
                            log.error("", e);
                            throw new RuntimeException(e);
                        }
                    }, boundedExecSvc);
                };
                Supplier<CompletionStage<String>> resilientFunc = Decorators.ofCompletionStage(nonResilientFunc)
                        .withCircuitBreaker(circuitBreaker)
                        // .withRetry(retry, unboundedExecSvc)
                        .withFallback(_ -> "Result (fallback) " + localI)
                        .decorate();
                var futResilientRes = resilientFunc.get().toCompletableFuture();

                futs.add(futResilientRes);
                log.info("Submitted future #{}", i);
            } catch (RejectedExecutionException ex) {
                numErrors++;
                log.error("Error submitting future #{}", i, ex);
            }
        }

        var numFallbacks = 0;
        for (var fut : futs) {
            try {
                var res = fut.join();
                if (res.contains("fallback")) {
                    numFallbacks++;
                }
                log.info(res);
            } catch (Exception ex) {
                numErrors++;
                log.error("Error obtaining future result", ex);
            }
        }
        log.info("Num requests: {}, num errors: {}, numFallbacks: {}", numRqs, numErrors, numFallbacks);

        assertEquals(numErrors, 0);
        assertTrue(numFallbacks > 0);
    }

    @Test
    void circuitBreakerAlsoTracksSlowCalls() {
        // so that if we don't want to fail on slow calls we must configure it (default
        // is 60s).
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofMillis(1))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(3)
                .slidingWindowType(SlidingWindowType.COUNT_BASED)
                .slowCallDurationThreshold(Duration.ofMillis(500L))
                .build();

        CircuitBreaker circuitBreaker = CircuitBreaker.of("cb", circuitBreakerConfig);
        var evtPub = circuitBreaker.getEventPublisher();
        evtPub.onEvent((CircuitBreakerEvent evt) -> {
            log.info(evt.toString());
        });

        RateLimiterConfig rateLimiterConfig = RateLimiterConfig.custom()
                .limitForPeriod(10)
                .limitRefreshPeriod(Duration.ofSeconds(1L))
                .build();
        RateLimiter rateLimiter = RateLimiter.of("rl", rateLimiterConfig);

        var execSvc = Executors.newFixedThreadPool(4);
        var futs = new ArrayList<CompletableFuture<String>>();
        int numRqs = 100;
        int numErrors = 0;

        for (int i = 0; i < numRqs; i++) {
            final int localI = i;
            try {
                Supplier<CompletionStage<String>> nonResilientFunc = () -> {
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            Thread.sleep(1000);
                            return "Result " + localI;
                        } catch (InterruptedException e) {
                            log.error("", e);
                            throw new RuntimeException(e);
                        }
                    }, execSvc);
                };
                // it will be failing due to "slow calls", and turned to fallbacks
                Supplier<CompletionStage<String>> resilientFunc = Decorators.ofCompletionStage(nonResilientFunc)
                        .withCircuitBreaker(circuitBreaker)
                        // we introduce submission rate limiter to let circuit breaker chance to detect
                        // slow calls (as each call takes 1 second to finish),
                        // and go into open state, otherwise we'll submit everything prior to circ br
                        // opens and it won't ever open.
                        .withRateLimiter(rateLimiter)
                        .withFallback(_ -> "Result (fallback) " + localI)
                        .decorate();
                var futRes = resilientFunc.get().toCompletableFuture();

                futs.add(futRes);
                log.info("Submitted future #{}", i);
            } catch (RejectedExecutionException ex) {
                numErrors++;
                log.error("Error submitting future #{}", i, ex);
            }
        }

        var numFallbacks = 0;
        for (var fut : futs) {
            try {
                var res = fut.join();
                if (res.contains("fallback")) {
                    numFallbacks++;
                }
                log.info(res);
            } catch (Exception ex) {
                numErrors++;
                log.error("Error obtaining future result", ex);
            }
        }
        log.info("Num requests: {}, num errors: {}, numFallbacks: {}", numRqs, numErrors, numFallbacks);

        assertEquals(numErrors, 0);
        assertTrue(numFallbacks > 0);
        
    }
}
