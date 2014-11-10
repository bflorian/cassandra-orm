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

package com.reachlocal.grails.plugins.cassandra.utils;

import java.util.UUID;

/**
 * @author: Bob Florian
 */
public class UuidHelper
{

	public static byte[] getBytes(int value)
	{
		byte[] buffer = new byte[4];
		for (int i = 0; i < 4; i++) {
			buffer[i] = (byte) (value >>> 8 * (7 - i));
		}
		return buffer;
	}

	public static byte[] getBytes(long value)  {
		byte[] buffer = new byte[8];
		for (int i = 0; i < 8; i++) {
			buffer[i] = (byte) (value >>> 8 * (7 - i));
		}
		return buffer;
	}

	public static UUID fromBytes(byte[] uuid)
	{
		long msb = 0;
		long lsb = 0;
		assert uuid.length == 16;
		for (int i=0; i<8; i++)
			msb = (msb << 8) | (uuid[i] & 0xff);
		for (int i=8; i<16; i++)
			lsb = (lsb << 8) | (uuid[i] & 0xff);

		com.eaio.uuid.UUID u = new com.eaio.uuid.UUID(msb,lsb);
		return java.util.UUID.fromString(u.toString());
	}

	public static byte[] getBytes(UUID uuid)
	{
		long msb = uuid.getMostSignificantBits();
		long lsb = uuid.getLeastSignificantBits();
		byte[] buffer = new byte[16];

		for (int i = 0; i < 8; i++) {
			buffer[i] = (byte) (msb >>> 8 * (7 - i));
		}
		for (int i = 8; i < 16; i++) {
			buffer[i] = (byte) (lsb >>> 8 * (7 - i));
		}

		return buffer;
	}

	public static long createTime(long currentTime) {
		long time;

		// UTC time
		long timeToUse = (currentTime * 10000) + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;

		// time low
		time = timeToUse << 32;

		// time mid
		time |= (timeToUse & 0xFFFF00000000L) >> 16;

		// time hi and version
		time |= 0x1000 | ((timeToUse >> 48) & 0x0FFF); // version 1
		return time;
	}

	public static long createTimeFromMicros(long currentTime) {
		long time;

		// UTC time
		long timeToUse = (currentTime * 10) + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;

		// time low
		time = timeToUse << 32;

		// time mid
		time |= (timeToUse & 0xFFFF00000000L) >> 16;

		// time hi and version
		time |= 0x1000 | ((timeToUse >> 48) & 0x0FFF); // version 1
		return time;
	}

	public static long createTimeFromNanos(long currentTime) {
		long time;

		// UTC time
		long timeToUse = currentTime + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;

		// time low
		time = timeToUse << 32;

		// time mid
		time |= (timeToUse & 0xFFFF00000000L) >> 16;

		// time hi and version
		time |= 0x1000 | ((timeToUse >> 48) & 0x0FFF); // version 1
		return time;
	}

	static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;
}
