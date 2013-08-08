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

package com.reachlocal.grails.plugins.cassandra.orm

import org.springframework.beans.factory.InitializingBean
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware

import com.reachlocal.grails.plugins.cassandra.mapping.DataMapping

/**
 * @author: Bob Florian
 */
class CassandraOrmService implements InitializingBean, ApplicationContextAware
{
	static transactional = false

	ApplicationContext applicationContext
	def grailsApplication
	def ormClientServiceName
	def client
	def persistence
	def mapping

	void afterPropertiesSet ()
	{
		ormClientServiceName = grailsApplication.config?.cassandra?.ormClientServiceName ?: "astyanaxService"
		client = applicationContext.getBean(ormClientServiceName)
		persistence = client.orm
		mapping = new DataMapping(persistence: persistence)
	}

	def defaultKeyspaceName(cluster)
	{
		client.defaultKeyspaceName(cluster)
	}

	def withKeyspace(keyspace, cluster, block)
	{
		client.withKeyspace(keyspace, cluster, block)
	}
}
