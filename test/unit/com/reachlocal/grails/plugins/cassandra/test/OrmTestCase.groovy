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

import org.codehaus.groovy.grails.commons.ConfigurationHolder
import com.reachlocal.grails.plugins.cassandra.orm.CassandraOrmService
import com.reachlocal.grails.plugins.cassandra.mapping.DataMapping
import com.reachlocal.grails.plugins.cassandra.mapping.ClassMethods
import com.reachlocal.grails.plugins.cassandra.test.orm.User
import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroup
import com.reachlocal.grails.plugins.cassandra.test.orm.UserGroupMeeting
import com.reachlocal.grails.plugins.cassandra.mapping.InstanceMethods

/**
 * @author: Bob Florian
 */
class OrmTestCase extends GroovyTestCase 
{
	def persistence
	def client
	def ctx

	void initialize()
	{
		ConfigurationHolder.config = [cassandra: [keySpace: 'mockDefault']]
		persistence = new MockPersistenceMethods()
		client = new CassandraOrmService(
				client: new Expando(
						execute: {keyspace, block -> block("context")},
				),
				persistence: persistence,
				mapping: new DataMapping(persistence: persistence)
		)

		ctx = new Expando(getBean: {name -> client})

		ClassMethods.addDynamicOrmMethods(User, ctx)
		ClassMethods.addDynamicOrmMethods(UserGroup, ctx)
		ClassMethods.addDynamicOrmMethods(UserGroupMeeting, ctx)

		InstanceMethods.addDynamicOrmMethods(User, ctx)
		InstanceMethods.addDynamicOrmMethods(UserGroup, ctx)
		InstanceMethods.addDynamicOrmMethods(UserGroupMeeting, ctx)
	}
}
