/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db;

import org.apache.cassandra.db.filter.TombstoneOverwhelmingException;
import org.apache.cassandra.heartbeat.readhandler.ReadHandler;
import org.apache.cassandra.heartbeat.status.ARResult;
import org.apache.cassandra.heartbeat.status.StatusMap;
import org.apache.cassandra.heartbeat.utils.ConfReader;
import org.apache.cassandra.heartbeat.utils.HBUtils;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tracing.Tracing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadVerbHandler implements IVerbHandler<ReadCommand>
{
    private static final Logger logger = LoggerFactory.getLogger( ReadVerbHandler.class );
    private Object lock = new Object();

    public void doVerb(MessageIn<ReadCommand> message, int id)
    {
        if (StorageService.instance.isBootstrapMode())
        {
            throw new RuntimeException("Cannot service reads while bootstrapping!");
        }

        ReadCommand command = message.payload;
        if (ConfReader.heartbeatEnable() && HBUtils.isValidRead(command))
        {
            ARResult result = StatusMap.instance.hasLatestValue(command);
            if (!result.value())
            {
                // sink subscription
                synchronized (lock)
                {
                    try
                    {
                        ReadHandler.sinkRead(command, lock, result);
                        lock.wait();
                    }
                    catch (Exception e)
                    {
                        HBUtils.error("Exception: {}", e.getMessage());
                    }
                }
            }
        }
        Keyspace keyspace = Keyspace.open(command.ksName);
        Row row;
        try
        {
            row = command.getRow(keyspace);
        }
        catch (TombstoneOverwhelmingException e)
        {
            // error already logged.  Drop the request
            return;
        }

        MessageOut<ReadResponse> reply = new MessageOut<ReadResponse>(MessagingService.Verb.REQUEST_RESPONSE,
                                                                      getResponse(command, row),
                                                                      ReadResponse.serializer);
        Tracing.trace("Enqueuing response to {}", message.from);
        MessagingService.instance().sendReply(reply, id, message.from);
    }

    public static ReadResponse getResponse(ReadCommand command, Row row)
    {
        if (command.isDigestQuery())
        {
            return new ReadResponse(ColumnFamily.digest(row.cf));
        }
        else
        {
            return new ReadResponse(row);
        }
    }
}
