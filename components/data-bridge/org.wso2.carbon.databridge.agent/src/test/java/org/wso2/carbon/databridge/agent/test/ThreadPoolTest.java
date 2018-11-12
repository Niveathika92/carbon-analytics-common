package org.wso2.carbon.databridge.agent.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.wso2.carbon.databridge.agent.endpoint.DataEndpoint;
import org.wso2.carbon.databridge.agent.endpoint.DataEndpointFailureCallback;
import org.wso2.carbon.databridge.agent.endpoint.EventPublisherThreadPoolExecutor;
import org.wso2.carbon.databridge.agent.exception.DataEndpointAuthenticationException;
import org.wso2.carbon.databridge.agent.exception.DataEndpointException;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.exception.SessionTimeoutException;
import org.wso2.carbon.databridge.commons.exception.UndefinedEventTypeException;

import java.util.List;

public class ThreadPoolTest {

    private static Log log = LogFactory.getLog(ThreadPoolTest.class);


    private class TestDataEndpoint extends DataEndpoint {
        EventPublisherThreadPoolExecutor testEventPublisherThreadPoolExecutor;
        DataEndpointFailureCallback dataEndpointFailureCallback;

        TestDataEndpoint() {
            testEventPublisherThreadPoolExecutor = new EventPublisherThreadPoolExecutor(1, 1, 1000, "testEventPublisherThreadPoolExecutor");
        }

        void registerDataEndpointFailureCallback(DataEndpointFailureCallback callback) {
            dataEndpointFailureCallback = callback;
        }

        @Override
        protected void send(Object client, List<Event> events) throws DataEndpointException, SessionTimeoutException, UndefinedEventTypeException {
            try {
                testEventPublisherThreadPoolExecutor.submitJobAndSetState(new Thread(new TestEventPublisher(null)), this);
            } catch (RuntimeException e) {
                //Ignore
            }
        }

        class TestEventPublisher extends EventPublisher implements Runnable {

            public TestEventPublisher(List<Event> events) {
                super(events);
            }

            @Override
            public void run() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {

                }
                try {
                    throw new RuntimeException("Throwing run time exception.");
                } catch (RuntimeException e) {
                    log.info("Retry from TestEventPublisher Runnable");
                    dataEndpointFailureCallback.tryResendEvents(null, null);
                }
            }
        }

        @Override
        protected String login(Object client, String userName, String password) throws DataEndpointAuthenticationException {
            return "";
        }

        @Override
        protected void logout(Object client, String sessionId) throws DataEndpointAuthenticationException {
        }

        @Override
        public String getClientPoolFactoryClass() {
            return "";
        }

        @Override
        public String getSecureClientPoolFactoryClass() {
            return "";
        }

    }


    public class TestDataEndpointGroup implements DataEndpointFailureCallback {
        TestDataEndpoint testDataEndpoint;

        TestDataEndpointGroup(TestDataEndpoint test) {
            testDataEndpoint = test;
            testDataEndpoint.registerDataEndpointFailureCallback(this);
        }


        public void send() {
            try {
                testDataEndpoint.send(null, null);
            } catch (Exception e) {
                //Ignore
            }
        }

        @Override
        public void tryResendEvents(List<Event> events, DataEndpoint failedEP) {
            try {
                testDataEndpoint.send(null, null);
            } catch (Exception e) {
                log.info("Try resend failed");
            }
        }
    }

    @Test
    public void testRunnable() throws InterruptedException {
        TestDataEndpoint testDataEndpoint = new TestDataEndpoint();
        TestDataEndpointGroup testDataEndpointGroup = new TestDataEndpointGroup(testDataEndpoint);

        // Call send method once - Goes into Semaphore Lock
        testDataEndpointGroup.send();
        Thread.sleep(5000);
        // Call send again to verify
        testDataEndpointGroup.send();
        Thread.sleep(100000);

    }
}
