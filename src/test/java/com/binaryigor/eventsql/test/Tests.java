package com.binaryigor.eventsql.test;

public class Tests {

    public static void awaitAssertion(Runnable assertion) {
        awaitAssertion(assertion, 3000);
    }

    public static void awaitAssertion(Runnable assertion, int waitLimit) {
        var singleWait = 100;
        var iterations = waitLimit / singleWait;
        try {
            AssertionError lastError = null;
            for (int i = 0; i < iterations; i++) {
                try {
                    assertion.run();
                    return;
                } catch (AssertionError e) {
                    lastError = e;
                    Thread.sleep(singleWait);
                }
            }
            throw lastError;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
