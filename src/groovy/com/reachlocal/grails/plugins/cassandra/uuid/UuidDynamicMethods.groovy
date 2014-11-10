/*
 * Copyright 2012 ReachLocal Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.reachlocal.grails.plugins.cassandra.uuid

import org.apache.commons.codec.binary.Base64
import com.eaio.uuid.UUIDGen // TODO -- replace with apache cassandra time UUID generator?
import com.reachlocal.grails.plugins.cassandra.utils.UuidHelper

/**
 * @author: Bob Florian
 */
class UuidDynamicMethods
{
	static void addAll()
	{
		Integer.metaClass.getBytes() {
			return UuidHelper.getBytes(delegate)
		}

		Long.metaClass.getBytes() {
			return UuidHelper.getBytes(delegate)
		}

		String.metaClass.toUUID() {
			return UUID.fromString(delegate)
		}

		UUID.metaClass.'static'.timeUUID = {
			return timeUUID()
		}

		UUID.metaClass.'static'.reverseTimeUUID = {
			reverseTimeUUID()
		}

		UUID.metaClass.'static'.timeUUID = {msec ->
			timeUUID(msec)
		}

		UUID.metaClass.'static'.timeUUID = {msec, microsec ->
			timeUUID(msec, microsec)
		}

		UUID.metaClass.'static'.maxTimeUUID = {msec ->
			maxTimeUUID(msec)
		}

		UUID.metaClass.'static'.maxTimeUUID = {msec, microsec ->
			maxTimeUUID(msec, microsec)
		}

		UUID.metaClass.'static'.minTimeUUID = {msec ->
			minTimeUUID(msec)
		}

		UUID.metaClass.'static'.minTimeUUID = {msec, microsec ->
			minTimeUUID(msec, microsec)
		}

		UUID.metaClass.'static'.fromBytes = {uuid ->
			return UuidHelper.fromBytes(uuid as byte[])
		}

		UUID.metaClass.getBytes = {
			return UuidHelper.getBytes(delegate)
		}

		UUID.metaClass.toUrlSafeString = {
			Base64.encodeBase64URLSafeString(delegate.bytes)
		}

		UUID.metaClass.getTime = {
			return time(delegate)
		}
	}

	static Long time(UUID uuid)
	{
		return (uuid.timestamp() - UuidHelper.NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000 as Long
	}

	static UUID timeUUID()
	{
		return new UUID(UUIDGen.newTime(), UUIDGen.getClockSeqAndNode())
	}

	static UUID timeUUID(long msec)
	{
		long t = UuidHelper.createTime(msec as long)
		return new UUID(t, UUIDGen.getClockSeqAndNode())
	}

	static UUID timeUUID(long msec, long microsec)
	{
		long t = UuidHelper.createTimeFromMicros(((msec * 1000L) + microsec) as long)
		return new UUID(t, UUIDGen.getClockSeqAndNode())
	}

	static UUID timeUUID(Date date)
	{
		long t = UuidHelper.createTime(date.time as long)
		return new UUID(t, UUIDGen.getClockSeqAndNode())
	}

	static UUID maxTimeUUID(long msec)
	{
		long t = UuidHelper.createTimeFromNanos(((msec + 1) * 10000L) - 1 as long)
		return new UUID(t, MAX_CLOCK_SEQ_AND_NODE)
	}

	static UUID maxTimeUUID(long msec, long microsec)
	{
		long t = UuidHelper.createTimeFromNanos((((msec * 1000L) + (microsec + 1)) * 10) - 1 as long)
		return new UUID(t, MAX_CLOCK_SEQ_AND_NODE)
	}

	static UUID maxTimeUUID(Date date)
	{
		long t = UuidHelper.createTimeFromNanos(((date.time + 1) * 10000L) - 1 as long)
		return new UUID(t, MAX_CLOCK_SEQ_AND_NODE)
	}

	static UUID minTimeUUID(long msec)
	{
		long t = UuidHelper.createTime(msec as long)
		return new UUID(t, MIN_CLOCK_SEQ_AND_NODE)
	}

	static UUID minTimeUUID(long msec, long microsec)
	{
		long t = UuidHelper.createTimeFromMicros((msec * 1000L) + microsec)
		return new UUID(t, MIN_CLOCK_SEQ_AND_NODE)
	}

	static UUID minTimeUUID(Date date)
	{
		long t = UuidHelper.createTime(date.time as long)
		return new UUID(t, MIN_CLOCK_SEQ_AND_NODE)
	}

	static UUID reverseTimeUUID()
	{
		long t = UuidHelper.createTimeFromMicros(((Long.MAX_VALUE - UUIDGen.newTime()) / 10L) as long)
		return new UUID(t, UUIDGen.getClockSeqAndNode())
	}

	private static final long MIN_CLOCK_SEQ_AND_NODE = 0x8080808080808080L;
	private static final long MAX_CLOCK_SEQ_AND_NODE = 0x7f7f7f7f7f7f7f7fL;

}
