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

package com.reachlocal.grails.plugins.cassandra.test;

import com.reachlocal.grails.plugins.cassandra.test.orm.User
import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroup
import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroupMeeting;


/**
 * @author: Bob Florian
 */
public class ClassMethodsTests extends OrmTestCase
{
	void testAll()
	{
		initialize()

		println "-- setup --"
		new User(
				uuid: "x1xx-xxxx-xxxx-xxxx",
				name: "Get Test",
				state:  "MD",
				phone: '301-555-1212',
				gender: 'Male',
				city:  'Ellicott City').save()

		new User(
				uuid: "x2xx-xxxx-xxxx-xxxx",
				name: "Get Test 2",
				email: "email2@local.com",
				state:  "VA", phone: '301-555-1212',
				gender: 'Female',
				city: 'Reston').save()

		new User(
				uuid: "x3xx-xxxx-xxxx-xxxx",
				name: "Get Test 3",
				state:  "MD",
				phone: '301-555-1234',
				gender: 'Female',
				city:  'Ellicott City').save()

		new User(
				uuid: "x4xx-xxxx-xxxx-xxxx",
				name: "Get Test 4",
				state:  "CA",
				phone: '301-555-1111',
				gender: 'Female',
				city:  'Pleasanton').save()

		new User(
				uuid: "x5xx-xxxx-xxxx-xxxx",
				name: "Get Test 5",
				state:  "MD",
				phone: '301-555-1212',
				gender: 'Male',
				city:  'Olney').save()

		persistence.printClear()

		println "\n--- getCassandra() ---"
		persistence.printClear()
		assertEquals client, User.cassandra

		println "\n--- getKeySpace() ---"
		persistence.printClear()
		assertEquals "mock", User.keySpace
		assertEquals "mockDefault", UserGroup.keySpace

		println "\n--- getColumnFamilyName() ---"
		persistence.printClear()
		assertEquals "MockUser", User.columnFamilyName
		assertEquals "UserGroup", UserGroup.columnFamilyName

		println "\n--- getColumnFamily() ---"
		persistence.printClear()
		assertEquals "MockUser_CFO", User.columnFamily
		assertEquals "UserGroup_CFO", UserGroup.columnFamily

		println "\n--- getIndexColumnFamily() ---"
		assertEquals "MockUser_IDX_CFO", User.indexColumnFamily
		assertEquals "UserGroup_IDX_CFO", UserGroup.indexColumnFamily
		persistence.printClear()

		println "\n--- belongsToClass(clazz) ---"
		persistence.printClear()
		assertTrue User.belongsToClass(UserGroup)
		assertFalse User.belongsToClass(UserGroupMeeting)

		println "\n--- get() ---"
		def r = User.get("x1xx-xxxx-xxxx-xxxx")
		persistence.printClear()
		println r
		assertEquals "Get Test", r.name

		println "\n--- list() ---"
		r = User.list()
		persistence.printClear()
		println r
		assertEquals 5, r.size()

		println "\n--- list(max: 2) ---"
		r = User.list(max: 2)
		persistence.printClear()
		println r
		assertEquals 2, r.size()

		println "\n--- list(start: 'x1', finish: 'x3') ---"
		r = User.list(start: 'x1', finish: 'x3')
		persistence.printClear()
		println r
		assertEquals 3, r.size()

		println "\n--- list(start: 'x4z', finish: 'x3z', reversed: true) ---"
		r = User.list(start: 'x4z', finish: 'x3z', reversed:  true) as List
		persistence.printClear()
		println r
		assertEquals 2, r.size()
		assertEquals "Get Test 4", r[0].name

		println "\n--- findAllWhere(state: 'MD') [secondary TO BE IMPLEMENTED] ---"
		r = User.findAllWhere(state: 'MD')
		persistence.printClear()
		println r

		println "\n--- findAllWhere(phone: '301-555-1212') [explicit] ---"
		r = User.findAllWhere(phone: '301-555-1212')
		persistence.printClear()
		println r
		assertEquals 3, r.size()
		
		println "\n--- findAllWhere(city: 'Olney', gender: 'Male') [secondary TO BE IMPLEMENTED] ---"
		r = User.findAllWhere(city: 'Olney', gender: 'Male')
		persistence.printClear()
		println r

		println "\n--- findWhere(phone: '301-555-1212') [explicit] ---"
		r = User.findWhere(phone: '301-555-1234')
		persistence.printClear()
		println r
		assertNotNull r

		println "\n--- findByPhone('301-555-1111') [explicit] ---"
		r = User.findByPhone('301-555-1111')
		persistence.printClear()
		println r
		assertNotNull r

		println "\n--- findAllByPhone('301-555-1212') [explicit] ---"
		r = User.findAllByPhone('301-555-1212')
		persistence.printClear()
		println r
		assertEquals 3, r.size()

		println "\n--- findAllByGender('Male') [secondary TO BE IMPLEMENTED] ---"
		r = User.findAllByGender('Male')
		persistence.printClear()
		println r

		println "\n--- findAllByCityAndGender('Ellicott City','Female',[max: 20, reversed:  true]) [explicit] ---"
		r = User.findAllByCityAndGender('Ellicott City','Female',[max: 20, reversed:  true])
		persistence.printClear()
		println r
		assertEquals 1, r.size()

		println "\n--- findAllByPhone('301-555-1212',[max: 2, column: 'name']) [explicit] ---"
		r = User.findAllByPhone('301-555-1212',[max: 2, column: 'name'])
		persistence.printClear()
		println r
		assertEquals 2, r.size()

		println "\n--- findAllByPhone('301-555-1111',[max: 2, rawColumn: 'name']) [explicit] ---"
		r = User.findAllByPhone('301-555-1111',[max: 2, rawColumn: 'name'])
		persistence.printClear()
		println r

		println "\n--- findAllByPhone('301-555-1111',[max: 2, columns: ['name','city']])[explicit] ---"
		r = User.findAllByPhone('301-555-1111',[max: 2, columns: ['name','city']])
		persistence.printClear()
		println r

		println "\n--- findAllByPhone('301-555-1111',[max: 2, rawColumns: ['name','city']])[explicit] ---"
		r = User.findAllByPhone('301-555-1111',[max: 2, rawColumns: ['name','city']])
		persistence.printClear()
		println r

		println "\n--- findAllByState('VA') [secondary TO BE IMPLEMENTED] ---"
		r = User.findAllByState('VA')
		persistence.printClear()
		println r

		println "\n--- findAllByGender('Female',[max: 50]) [secondary TO BE IMPLEMENTED] ---"
		r = User.findAllByGender('Female',[max: 50])
		persistence.printClear()
		println r

		println "\n--- countByPhone('301-555-1212') [explicit] ---"
		r = User.countByPhone('301-555-1212')
		persistence.printClear()
		println r
		assertEquals 3, r

		println "\n--- countByGender('Male') [explicit TO BE IMPLEMENTED] ---"
		r = User.countByGender('Male')
		persistence.printClear()
		println r

		println "\n--- countByCityAndGender('Ellicott City','Male',[max: 20, reversed:  true]) [explicit] ---"
		r = User.countByCityAndGender('Ellicott City','Male',[max: 20, reversed:  true])
		persistence.printClear()
		println r
		assertEquals 1, r
	}
}
