import com.reachlocal.grails.plugins.cassandra.mapping.OrmUtility
import com.reachlocal.grails.plugins.cassandra.uuid.UuidDynamicMethods

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

class CassandraOrmGrailsPlugin
{
	// the plugin version
	def version = "0.2.3"

	// the version or versions of Grails the plugin is designed for
	def grailsVersion = "2.0.0 > *"

	// the other plugins this plugin depends on
	def dependsOn = [:]

	// resources that are excluded from plugin packaging
	def pluginExcludes = [
			"grails-app/views/error.gsp"
	]


	def author = "Bob Florian"
	def authorEmail = "bob.florian@reachlocal.com"
	def title = "Cassandra Persistence Framework"
	def license = 'APACHE'
	def organization = [name: 'ReachLocal', url: 'http://www.reachlocal.com/']
	def issueManagement = [system: 'JIRA', url: 'http://jira.grails.org/browse/GPCASSANDRAASTYANAX']
	def scm = [url: 'https://github.com/bflorian/cassandra-orm']

	def description = '''\\
Provides GORM-like dynamic methods for persisting Groovy objects into Cassandra (does not implement the GORM API).
'''

	// URL to the plugin's documentation
	def documentation = "http://grails.org/plugin/cassandra-orm"

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
