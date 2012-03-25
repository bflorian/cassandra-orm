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

import com.reachlocal.grails.plugins.cassandra.test.orm.User
import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroup
import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroupMeeting

/**
 * @author: Bob Florian
 */
class InstanceMethodTests extends OrmTestCase
{
	void testAll()
	{
		initialize()

		def userGroup = new UserGroup(uuid: "group1-zzzz-zzzz", name: "JUG")
		def userGroup2 = new UserGroup(uuid: "group2-zzzz-zzzz", name: "CUG")
		def user = new User(uuid: "user1-zzzz-zzzz", name: "Jane", phone:  "301-555-2222", city: "Reston", state:  "VA", gender:  "Female")

		println "\n--- getCassandra() ---"
		assertEquals client, user.cassandra
		persistence.printClear()

		println "\n--- getKeySpace() ---"
		assertEquals "mock", user.keySpace
		assertEquals "mockDefault", userGroup.keySpace
		persistence.printClear()

		println "\n--- getColumnFamily() ---"
		assertEquals "MockUser_CFO", user.columnFamily
		assertEquals "UserGroup_CFO", userGroup.columnFamily
		persistence.printClear()

		println "\n--- getIndexColumnFamily() ---"
		assertEquals "MockUser_IDX_CFO", user.indexColumnFamily
		assertEquals "UserGroup_IDX_CFO", userGroup.indexColumnFamily
		persistence.printClear()

		println "\n--- getId() ---"
		assertEquals "user1-zzzz-zzzz", user.id
		persistence.printClear()
		println user.id

		println "\n--- save() ---"
		userGroup.save()
		persistence.printClear()
		println user.id

		println "\n--- delete() ---"
		userGroup.delete()
		persistence.printClear()

		println "\n--- userGroup2.users ---"
		def r = userGroup2.users
		persistence.printClear()
		println r

		println "\n--- userGroup.users(max: 5) ---"
		r = userGroup.users(max: 5)
		persistence.printClear()
		println user.id
		println r

		println "\n--- userGroup.users(max: 50, column: 'name') ---"
		r = userGroup.users(max: 50, column: 'name')
		persistence.printClear()
		println user.id
		println r

		println "\n--- userGroup.users(max: 50, columns: ['name','city']) ---"
		r = userGroup.users(max: 50, columns: ['name','city'])
		persistence.printClear()
		println user.id
		println r

		println "\n--- userGroup.usersCount() ---"
		r = userGroup.usersCount()
		persistence.printClear()
		println r

		println "\n--- userGroup.usersCount(start: 'x1', finish: 'x2') ---"
		r = userGroup.usersCount(start: 'x1', finish: 'x2')
		persistence.printClear()
		println r

		println "\n--- userGroup.addToUsers(user) ---"
		userGroup.addToUsers(user)
		persistence.printClear()

		println "\n--- userGroup.removeFromUsers(user) ---"
		userGroup.removeFromUsers(user)
		persistence.printClear()

		println "\n--- user.userGroup ---"
		user.userGroup
		persistence.printClear()

		println "\n--- user.userGroup = userGroup2 ---"
		user.userGroup = userGroup2
		persistence.printClear()
		assertEquals userGroup2, user.userGroup
	}
}
