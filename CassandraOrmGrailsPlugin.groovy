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

import com.reachlocal.grails.plugins.cassandra.mapping.OrmUtility
import com.reachlocal.grails.plugins.cassandra.uuid.UuidDynamicMethods

class CassandraOrmGrailsPlugin
{
	def version = "1.2.5"
	def grailsVersion = "2.0.0 > *"
	def author = "Bob Florian"
	def authorEmail = "bob.florian@reachlocal.com"
	def title = "Cassandra Object Persistence Framework"
	def license = 'APACHE'
	def organization = [name: 'ReachLocal', url: 'http://www.reachlocal.com/']
	def issueManagement = [system: 'JIRA', url: 'http://jira.grails.org/browse/GPCASSANDRAORM']
	def scm = [url: 'https://github.com/bflorian/cassandra-orm']
	def documentation = "http://bflorian.github.io/cassandra-orm/"

	def description = '''\
Provides GORM-like dynamic methods for persisting Groovy objects into Cassandra (but does not implement the GORM API).
It also adds a number of dynamic methods and arguments specific to typical Cassandra usage, like the ability to specify consistency level and manipulate counters.
Must me used in concert with the cassandra-astyanax plugin.
'''

	def doWithDynamicMethods = { ctx ->
		UuidDynamicMethods.addAll()

		application.allClasses.each {clazz ->
			try {
				if (OrmUtility.isMappedClass(clazz)) {
					log.debug "Mapping ${clazz.name} for Casssandra ORM"
					OrmUtility.addDynamicMethods(clazz, ctx)
				}
			}
			catch (Exception e) {
				log.error "Exception adding ORM methods to ${clazz.name}"
			}
		}
	}
}
