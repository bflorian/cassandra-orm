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
import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroupPost
import org.junit.Test;
import static org.junit.Assert.*

/**
 * @author: Bob Florian
 */
class InstanceMethodTests extends OrmTestCase
{
	@Test
	void testAll()
	{
		initialize()

		def userGroup = new UserGroup(
				uuid: "group1-zzzz-zzzz",
				name: "JUG",
				color: "Blue")

		def userGroup2 = new UserGroup(
				uuid: "group2-zzzz-zzzz",
				name: "CUG")

		def userGroup3 = new UserGroup(
				uuid: "group3-zzzz-zzzz",
				name: "GUG",
				color: "Green")

		def user = new User(
				uuid: "user1-zzzz-zzzz",
				name: "Jane",
				city: "Reston",
				state:  "VA",
				username: "janer")

		def user2 = new User(
				uuid: "user2-zzzz-zzzz",
				name: "Jim",
				phone:  "301-555-1111",
				city: "Olney",
				state:  "MD",
				gender:  "Male",
				username: "jimo")

		def user3 = new User(
				uuid: "user3-zzzz-zzzz",
				name: "Jill",
				phone:  "301-555-1111",
				city: "Olney",
				state:  "MD",
				gender:  "Female",
				username:  "jillo")

		def user4 = new User(
				uuid: "user4-zzzz-zzzz",
				name: "John",
				phone:  "301-555-1212",
				city: "Ellicott City",
				state:  "MD",
				gender:  "Male",
				username:  "johne")

		// separate cluster
		def userGroup1a = new UserGroup(
				uuid: "group1-zzzz-2222",
				name: "JUG",
				color: "Blue")


		def meeting1 = new UserGroupMeeting(date:  new Date())
		def meeting2 = new UserGroupMeeting(date:  new Date()+1)
		def meeting3 = new UserGroupMeeting(date:  new Date()+2)

		def post1 = new UserGroupPost(text: "Four score")
		def post2 = new UserGroupPost(text: "and seven years ago")

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


		println "\n--- userGroup.save() ---"
		userGroup.save()
		persistence.printClear()

		println "\n--- userGroup.save(consistencyLevel: 'CL_LOCAL_QUORUM') ---"
		userGroup.save(consistencyLevel: 'CL_LOCAL_QUORUM')
		assertEquals 'CL_LOCAL_QUORUM', persistence.firstCall.args[-1]
		persistence.printClear()

		println "\n--- userGroup3.save(ttl: [color:  20]) ---"
		userGroup3.save(ttl: [color:  20])
		persistence.printClear()

		println "\n--- userGroup3.save(ttl: 60) ---"
		userGroup3.save(ttl: 60)
		persistence.printClear()

		println "\n--- userGroup.insert(name: JUG2) ---"
		userGroup.insert(name: 'JUG2')
		persistence.printClear()

		println "\n--- userGroup.insert(name: JUG2) ---"
		userGroup3.insert([name: 'GUG2'], 75)
		persistence.printClear()

		println "\n--- userGroup.color [EXPANDO] ---"
		println userGroup.color
		assertEquals "Blue", userGroup.color

		println "\n--- userGroup.flavor = 'Cinnamon' [EXPANDO] ---"
		userGroup.flavor = 'Cinnamon'
		assertEquals "Cinnamon", userGroup.flavor
		userGroup.save()
		persistence.printClear()

		println "\n--- userGroup.color & userGroup.flavor AFTER GET [EXPANDO] ---"
		def g = UserGroup.get("group1-zzzz-zzzz")
		persistence.printClear()
		assertEquals "Blue", g.color
		assertEquals "Cinnamon", g.flavor

		println "\n--- userGroup.addToUsers(user) ---"
		userGroup.addToUsers(user)
		persistence.printClear()

		println "\n--- userGroup.addToUsers(user2) ---"
		userGroup.addToUsers(user2)
		persistence.printClear()

		println "\n--- userGroup.addToUsers(user3) ---"
		userGroup.addToUsers(user3)
		persistence.printClear()


		println "\n--- userGroup1a.save(cluster: 'mockCluster2') ---"
		userGroup1a.save(cluster: 'mockCluster2')
		persistence.printClear()
		userGroup1a.addToUsers(user)
		userGroup1a.addToUsers(user2)
		persistence.printClear()
		def u = userGroup1a.users
		assertEquals 2, u.size()
		def g2 = UserGroup.get("group1-zzzz-2222")
		assertNull g2
		g2 = UserGroup.get("group1-zzzz-2222", [cluster: 'mockCluster2'])
		assertNotNull g2
		assertEquals 2, g2.users.size()

		println "\n--- userGroup2.save() ---"
		userGroup2.save()
		persistence.printClear()
		println user.id

		println "\n--- userGroup2.addToUsers(user4) ---"
		userGroup2.addToUsers(user4)
		persistence.printClear()


		println "\n--- userGroup.users ---"
		def r = userGroup.users
		persistence.printClear()
		println r
		assertEquals 3, r.size()

		println "\n--- userGroup.users(max: 2) ---"
		r = userGroup.users(max: 2)
		persistence.printClear()
		println r
		assertEquals 2, r.size()

		println "\n--- userGroup.users(max: 50, column: 'name') ---"
		r = userGroup.users(max: 2, column: 'name') as List
		persistence.printClear()
		println r
		assertEquals 2, r.size()
		assertEquals "Jane", r[0]


		println "\n--- userGroup.users(columns: ['name','city']) ---"
		r = userGroup.users(columns: ['name','city']) as List
		persistence.printClear()
		println r
		assertEquals 3, r.size()
		assertEquals "Olney", r[-1].city
		assertNull r[-1].state

		println "\n--- userGroup.usersCount() ---"
		r = userGroup.usersCount()
		persistence.printClear()
		assertEquals 3, r

		println "\n--- userGroup.usersCount(start: 'user1', finish: 'user2') ---"
		r = userGroup.usersCount(start: 'user1', finish: 'user2')
		persistence.printClear()
		assertEquals 2, r

		println "\n--- userGroup.removeFromUsers(user) ---"
		userGroup.removeFromUsers(user)
		persistence.printClear()

		println "\n--- userGroup.usersCount() ---"
		r = userGroup.usersCount()
		persistence.printClear()
		assertEquals 2, r

		println "\n--- delete() ---"
		userGroup.delete()
		persistence.printClear()


		println "\n--- userGroup2.addToMeetings(meeting1) ---"
		userGroup2.addToMeetings(meeting1)
		persistence.printClear()

		println "\n--- userGroup2.addToMeetings(meeting2) ---"
		userGroup2.addToMeetings(meeting2)
		persistence.printClear()

		println "\n--- userGroup.save() ---"
		userGroup.save()
		persistence.printClear()
		println user.id

		println "\n--- userGroup.addToMeetings(meeting3) ---"
		userGroup.addToMeetings(meeting3)
		persistence.printClear()


		println "\n--- userGroup.meetings ---"
		r = userGroup.meetings
		persistence.printClear()
		assertEquals 1, r.size()
		assertTrue r instanceof List

		println "\n--- userGroup2.meetings ---"
		r = userGroup2.meetings
		persistence.printClear()
		assertEquals 2, r.size()


		println "\n--- userGroup.addToPosts(post1) ---"
		userGroup.addToPosts(post1)
		persistence.printClear()

		println "\n--- userGroup.addToPosts(post2) ---"
		userGroup.addToPosts(post2)
		persistence.printClear()

		println "\n--- userGroup.posts ---"
		r = userGroup.posts
		persistence.printClear()
		assertEquals 2, r.size()
		assertTrue r instanceof Set

		println "\n--- user4.userGroup ---"
		r = user4.userGroup
		persistence.printClear()
		println "${r} (${r?.uuid})"
		assertNotNull r

		println "\n--- user4.userGroupId ---"
		r = user4.userGroupId
		persistence.printClear()
		println r
		assertEquals "group2-zzzz-zzzz", r

		println "\n--- user4.userGroup = userGroup2 ---"
		user4.userGroup = userGroup
		persistence.printClear()
		assertEquals userGroup, user4.userGroup

		println "\n--- user4.userGroupId ---"
		r = user4.userGroupId
		persistence.printClear()
		println r
		assertEquals "group1-zzzz-zzzz", r

		println "\n"
		r = UserGroupMeeting.list()
		persistence.printClear()
		assertEquals 6, r.size()
		userGroup2.delete(cascade: true)
		persistence.printClear()
		r = UserGroupMeeting.list()
		assertEquals 4, r.size()
	}
}
