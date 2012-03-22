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

		println "\n--- getCassandra() ---"
		assertEquals client, User.cassandra
		persistence.printClear()

		println "\n--- getKeySpace() ---"
		assertEquals "mock", User.keySpace
		assertEquals "mockDefault", UserGroup.keySpace
		persistence.printClear()

		println "\n--- getColumnFamilyName() ---"
		assertEquals "MockUser", User.columnFamilyName
		assertEquals "UserGroup", UserGroup.columnFamilyName
		persistence.printClear()

		println "\n--- getColumnFamily() ---"
		assertEquals "MockUser_CFO", User.columnFamily
		assertEquals "UserGroup_CFO", UserGroup.columnFamily
		persistence.printClear()

		println "\n--- getIndexColumnFamily() ---"
		assertEquals "MockUser_IDX_CFO", User.indexColumnFamily
		assertEquals "UserGroup_IDX_CFO", UserGroup.indexColumnFamily
		persistence.printClear()

		println "\n--- belongsToClass(clazz) ---"
		assertTrue User.belongsToClass(UserGroup)
		assertFalse User.belongsToClass(UserGroupMeeting)
		persistence.printClear()

		println "\n--- get() ---"
		def r = User.get("xxxx-xxxx-xxxx-xxxx")
		persistence.printClear()
		println r
		
		println "\n--- list() ---"
		r = User.list()
		persistence.printClear()
		println r
		
		println "\n--- list(max: 10) ---"
		r = User.list(max: 10)
		persistence.printClear()
		println r
		
		println "\n--- list(start: 'x1', finish: 'x2', reversed: true) ---"
		r = User.list(start: 'x1', finish: 'x2', reversed: true)
		persistence.printClear()
		println r

		println "\n--- findAllWhere(state: 'MD') [secondary TO BE IMPLEMENTED] ---"
		r = User.findAllWhere(state: 'MD')
		persistence.printClear()
		println r

		println "\n--- findAllWhere(phone: '301-555-1212') [explicit] ---"
		r = User.findAllWhere(phone: '301-555-1212')
		persistence.printClear()
		println r
		
		println "\n--- findAllWhere(city: 'Olney', gender: 'Female') [secondary] ---"
		r = User.findAllWhere(city: 'Olney', gender: 'Female')
		persistence.printClear()
		println r

		println "\n--- findWhere(phone: '301-555-1212') [explicit] ---"
		r = User.findWhere(phone: '301-555-1234')
		persistence.printClear()
		println r

		println "\n--- findByPhone('301-555-1111') [explicit] ---"
		r = User.findByPhone('301-555-1111')
		persistence.printClear()
		println r

		println "\n--- findAllByPhone('301-555-1111') [explicit] ---"
		r = User.findAllByPhone('301-555-1111')
		persistence.printClear()
		println r

		println "\n--- findAllByCityAndGender('Ellicott City','Male',[max: 20, reversed:  true]) [explicit] ---"
		r = User.findAllByCityAndGender('Ellicott City','Male',[max: 20, reversed:  true])
		persistence.printClear()
		println r

		println "\n--- findAllByPhone('301-555-1111',[max: 3, column: 'name']) [explicit] ---"
		r = User.findAllByPhone('301-555-1111',[max: 3, column: 'name'])
		persistence.printClear()
		println r

		println "\n--- findAllByPhone('301-555-1111',[max: 3, rawColumn: 'name']) [explicit] ---"
		r = User.findAllByPhone('301-555-1111',[max: 3, rawColumn: 'name'])
		persistence.printClear()
		println r

		println "\n--- findAllByPhone('301-555-1111',[max: 3, columns: ['name','city']])[explicit] ---"
		r = User.findAllByPhone('301-555-1111',[max: 3, columns: ['name','city']])
		persistence.printClear()
		println r

		println "\n--- findAllByPhone('301-555-1111',[max: 3, rawColumns: ['name','city']])[explicit] ---"
		r = User.findAllByPhone('301-555-1111',[max: 3, rawColumns: ['name','city']])
		persistence.printClear()
		println r

		println "\n--- findAllByState('VA') [secondary] ---"
		r = User.findAllByState('VA')
		persistence.printClear()
		println r

		println "\n--- findAllByGender('Female',[max: 50]) [secondary] ---"
		r = User.findAllByGender('Female',[max: 50])
		persistence.printClear()
		println r		
	}
}
