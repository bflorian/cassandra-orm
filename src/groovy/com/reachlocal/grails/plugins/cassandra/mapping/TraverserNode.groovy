package com.reachlocal.grails.plugins.cassandra.mapping

/**
 * @author: Bob Florian
 */
class TraverserNode 
{
	Traverser traverser
	TraverserNode parent
	String name
	def args
	def result

	def propertyMissing(String name)
	{
		return new TraverserNode(traverser: traverser, parent: this, name: name)
	}

	def methodMissing(String name, args)
	{
		return new TraverserNode(traverser : traverser, parent: this, name: name, args: args)
	}

	def path()
	{
		parent ? parent.path() + [this] : [this]
	}

	def evaluate(objects)
	{
		//println "EVALUATE: ${objects}"
		result = []
		if (args) {
			objects.each {obj ->
				//println "METHOD $name ($args)"
				result += obj.invokeMethod(name, args)
			}
		}
		else {
			objects.each {obj ->
				//println "PROPERTY: $name, value='${it.getProperty(name)}', object=${it.getClass().name}"
				result += obj.getProperty(name)
			}
		}
		return result
	}

	def execute(args=[:])
	{
		def segments = path()
		def items = [traverser.object]
		segments.each {n ->
			items = n.evaluate(items)
		}
		if (args.sort == null || args.sort) {
			items = items.sort{it.id}
		}
		if (args.reversed) {
			items = items.reverse()
		}
		if (args.max) {
			items = items[0..args.max-1]
		}
		return items
	}

	String toString()
	{
		"$name($args)"
	}
}
