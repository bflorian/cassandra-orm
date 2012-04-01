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

/**
 * @author: Bob Florian
 */
class UuidDynamicMethods 
{
	static void addAll()
	{
		Integer.metaClass.getBytes() {
			byte[] buffer = new byte[4];
			for (int i = 0; i < 4; i++) {
				buffer[i] = (byte) (delegate >>> 8 * (7 - i));
			}
			return buffer
		}

		Long.metaClass.getBytes() {
			byte[] buffer = new byte[8];
			for (int i = 0; i < 8; i++) {
				buffer[i] = (byte) (delegate >>> 8 * (7 - i));
			}
			return buffer
		}

		UUID.metaClass.'static'.timeUUID = {
			def u1 = new com.eaio.uuid.UUID()
			return new UUID(u1.time, u1.clockSeqAndNode)
		}

		UUID.metaClass.'static'.reverseTimeUUID = {
			def u1 = new com.eaio.uuid.UUID()
			return new UUID(Long.MAX_VALUE - u1.time, u1.clockSeqAndNode)
		}

		UUID.metaClass.'static'.fromBytes = {uuid ->
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

		UUID.metaClass.getBytes = {
			def uuid = delegate
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

		UUID.metaClass.toUrlSafeString = {
			Base64.encodeBase64URLSafeString(delegate.bytes)
		}
	}
}
