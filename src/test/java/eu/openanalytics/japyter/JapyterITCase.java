/*******************************************************************************
 * Copyright (c) 2015-2016 Open Analytics NV and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package eu.openanalytics.japyter;

import static eu.openanalytics.japyter.client.Shell.InspectDetailLevel.FINE;
import static eu.openanalytics.japyter.model.gen.HistoryRequest.HistAccessType.TAIL;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

import org.apache.commons.codec.digest.HmacAlgorithms;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.openanalytics.japyter.client.IoPub.BroadcastListener;
import eu.openanalytics.japyter.client.IoPub.MessageListener;
import eu.openanalytics.japyter.client.Shell;
import eu.openanalytics.japyter.client.Stdin.AbstractStdinHandler;
import eu.openanalytics.japyter.client.Stdin.StdinHandler;
import eu.openanalytics.japyter.model.Message;
import eu.openanalytics.japyter.model.gen.Broadcast;
import eu.openanalytics.japyter.model.gen.ExecuteRequest;
import eu.openanalytics.japyter.model.gen.HistoryRequest;

/**
 * Run the following before testing this:
 *
 * <pre>
 * <code>
 * docker run --net host \
 *      -e "PASSWORD=pwd" -e "USE_HTTP=1" \
 *      -v /tmp/ipython-docker/:/root/.ipython/ \
 *      ipython/notebook
 * </code>
 * </pre>
 *
 * Then:
 * <ul>
 * <li>Browse <a href="http://localhost:8888/">http://localhost:8888/</a></li>
 * <li>Log in using <code>pwd</code> as the password,
 * <li>Create a Python notebook,
 * <li>Allow the current user to read its config:
 *
 * <pre>
 * <code>
 * sudo chmod +xr -R /tmp/ipython-docker/
 * </code>
 * </pre>
 *
 * </li>
 * </ul>
 */
public class JapyterITCase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(JapyterITCase.class);

    // TODO turn this into a real JUnit driven integration test

    public static void main(final String[] args) throws Exception
    {
        final File configsDir = new File("/tmp/ipython-docker/profile_default/security/");

        final File[] configs = configsDir.listFiles(new FilenameFilter()
        {
            @Override
            public boolean accept(final File dir, final String name)
            {
                return name.matches("kernel-[^.]+\\.json");
            }
        });

        Validate.notNull(configs, "File to read configs in: " + configsDir
                                  + ". Does the current user have permission to do so?");

        Validate.isTrue(configs.length > 0,
            "At least one notebook must be running before running integration tests");

        final StdinHandler randomStdinHandler = new AbstractStdinHandler()
        {
            @Override
            public String prompt(final String text, final boolean password)
            {
                LOGGER.info("\nSTDIN Request: {} (password? {})", text, password);
                return RandomStringUtils.randomAlphanumeric(20);
            }
        };

        try (Japyter japyter = Japyter.fromConfigFile(configs[0])
            .withUserName("it-test")
            .withStdinHandler(randomStdinHandler)
            .build())
        {
            LOGGER.info("Running tests with Jupiter config: {}", configs[0]);

            // to test the creation of a Japyter instance by connecting to a known
            // shell or control URI, we build a URI from the currently active
            // connection, which is obviously correct since we've reached
            // this part of the code
            testSessionFromUri(new URI(japyter.getConfig().getTransport() + "://"
                                       + japyter.getConfig().getIp() + ":"
                                       + japyter.getConfig().getShellPort()), japyter.getSession()
                .getProtocol()
                .getHmacKey(), japyter.getSession().getProtocol().getHmacAlgorithm());

            LOGGER.info("\n\n*** SHELL TESTS ***\n");
            final Shell shell = japyter.getShell();
            LOGGER.info("ConnectReply: " + shell.connect());
            LOGGER.info("KernelInfoReply: " + shell.kernelInfo());

            // assumes a Python notebook running
            LOGGER.info("ExecuteReply: " + shell.execute(new ExecuteRequest().withCode("3+7")));
            LOGGER.info("InspectReply: " + shell.inspect("123.", 4, FINE));
            LOGGER.info("CompleteReply: " + shell.complete("123.", 4));
            LOGGER.info("IsCompleteReply: " + shell.isComplete("123."));
            LOGGER.info("HistoryReply: "
                        + shell.history(new HistoryRequest().withHistAccessType(TAIL).withN(10)));

            LOGGER.info("\n\n*** CONTROL TESTS ***\n");
            LOGGER.info("ConnectReply: " + japyter.getControl().shutdown(true));

            LOGGER.info("\n\n*** IO_PUB TESTS ***\n");
            japyter.getIoPub().subscribe(new BroadcastListener()
            {
                @Override
                public void handle(final Broadcast b)
                {
                    LOGGER.info("IoPub Broadcast: " + b);
                }
            });
            japyter.getIoPub().subscribe(new MessageListener()
            {
                @Override
                public void handle(final Message m)
                {
                    LOGGER.info("IoPub Message: " + m);
                }
            });

            LOGGER.info("Heartbeat state: {}\n", japyter.getHeartbeat().getState());

            LOGGER.info("\n\nType ENTER to stop\n");
            waitEnter();

            LOGGER.info("Heartbeat state: {}\n", japyter.getHeartbeat().getState());
            LOGGER.info("\n\nStopping...");
        }
        catch (final Throwable t)
        {
            LOGGER.error("Failed to run tests", t);
        }
        finally
        {
            System.exit(0);
        }
    }

    private static void testSessionFromUri(final URI shellUri,
                                           final byte[] hmacKey,
                                           final HmacAlgorithms hmacAlgorithm) throws IOException
    {
        LOGGER.info("\n\n*** CONNECT BY URI ***\n");
        LOGGER.info("Attempting connection by URI: {}...", shellUri);

        try (Japyter japyter = Japyter.fromUri(shellUri, hmacKey, hmacAlgorithm)
            .withUserName("it-test2")
            .build())
        {
            Validate.notNull(japyter.getControl());
            Validate.notNull(japyter.getHeartbeat());
            Validate.notNull(japyter.getIoPub());
            Validate.notNull(japyter.getSession());
            Validate.notNull(japyter.getShell());
            Validate.isTrue(japyter.getStdin() == null);

            LOGGER.info("All good! Closing test session now...\n");
        }
    }

    private static void waitEnter()
    {
        try (Scanner s = new Scanner(System.in))
        {
            s.nextLine();
        }
    }
}
