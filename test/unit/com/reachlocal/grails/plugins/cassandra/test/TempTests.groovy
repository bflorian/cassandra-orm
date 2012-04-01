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

package com.reachlocal.grails.plugins.cassandra.test

import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroup
import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroupMeeting

/**
 * @author: Bob Florian
 */
class TempTests extends OrmTestCase
{
	void testDelete()
	{
		initialize()

		def userGroup = new UserGroup(uuid: "group1-zzzz-zzzz", name: "JUG")
		def meeting1 = new UserGroupMeeting(date:  new Date())

		println "\nuserGroup.save()"
		userGroup.save()
		persistence.printClear()

		println "\nUserGroup.get('group1-zzzz-zzzz')"
		def g = UserGroup.get('group1-zzzz-zzzz')
		assertEquals "JUG", g.name
		persistence.printClear()

		println "\nuserGroup.addToMeetings(meeting1)"
		userGroup.addToMeetings(meeting1)
		persistence.printClear()

		println "\nUserGroupMeeting.get([userGroup, meeting1.date])"
		def m1a = UserGroupMeeting.get([meeting1.date, meeting1.uuid])
		persistence.printClear()
		assertNotNull m1a
		assertEquals meeting1.uuid, m1a.uuid

		println "\nm1a.userGroup"
		def ug1a = m1a.userGroup
		assertEquals userGroup.uuid, ug1a.uuid

		println "\n--- delete() ---"
		userGroup.delete()
		persistence.printClear()
	}
}
