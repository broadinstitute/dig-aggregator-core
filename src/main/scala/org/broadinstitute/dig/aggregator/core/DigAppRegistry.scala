package org.broadinstitute.dig.aggregator.core

/**
 * Every DigApp is required to have a unique name associated with it as this
 * name is used as the key for various database queries.
 *
 * This object is an attempt to enforce some uniqueness across applications.
 * Since all the processor apps are under a single umbrella project, each
 * can be registered with the registry globally:
 *
 * final object ProcessorApps {
 *   val variantProcessor = DigAppRegistry += "VP" -> classOf[VariantProcessor]
 *   val commitProcessor = DigAppRegistry += "CP" -> classOf[CommitProcessor]
 * }
 *
 * When the DigApp is created and the application name passed to it, it can
 * verify that:
 *
 *   1. the application name is valid; and
 *   2. the instance of DigApp instantiated matches the application name
 */
final object DigAppRegistry {

  /**
   * All registered apps.
   */
  private var registeredApps = Map[String, Class[_ <: DigApp]]()

  /**
   * Lookup the class of an application by name.
   */
  def apply(appName: String): Option[Class[_ <: DigApp]] = synchronized {
    registeredApps.get(appName)
  }

  /**
   * Add a new application to the registry.
   */
  def +=(app: (String, Class[_ <: DigApp])): String = synchronized {
    val name       = app._1
    val registered = registeredApps.get(name)

    // make sure it's not already registered
    require(registered.isEmpty, s"$name already registered to ${registered.get}")

    // register, return the name
    registeredApps += app
    name
  }
}

/**
 *
 */
final case class RegisteredApp(appName: String, clazz: Class[_ <: DigApp]) {

  // add this application to the registry
  DigAppRegistry += appName -> clazz

  /**
   * Returns the application name.
   */
  override def toString: String = appName
}
