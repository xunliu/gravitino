/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  id("java")
  alias(libs.plugins.shadow)
}

repositories {
  mavenCentral()
}

dependencies {
  implementation(project(":api"))
  implementation(project(":clients:client-java"))
  implementation(project(":common"))
  implementation(project(":core")) {
    exclude(group = "org.rocksdb", module = "rocksdbjni")
  }
  implementation(project(":server")) {
    exclude(group = "org.rocksdb", module = "rocksdbjni")
  }
  implementation(project(":server-common")) {
    exclude(group = "org.rocksdb", module = "rocksdbjni")
  }
  implementation(libs.bundles.jetty)
  implementation(libs.bundles.jersey)
  implementation(libs.bundles.jwt)
  implementation(libs.bundles.log4j)
  implementation(libs.commons.cli)
  implementation(libs.commons.lang3)
  implementation(libs.commons.io)
  implementation(libs.guava)
  implementation(libs.httpclient5)

  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.engine)
  testImplementation(libs.junit.jupiter.params)
}

tasks.withType<ShadowJar>(ShadowJar::class.java) {
  isZip64 = true
  configurations = listOf(
    project.configurations.runtimeClasspath.get()
  )
  archiveClassifier.set("")

  // avoid conflict with Spark test
  exclude("org/apache/logging/slf4j/**")
  relocate("org.eclipse.jetty", "org.apache.gravitino.shaded.org.eclipse.jetty")
  mergeServiceFiles()
}

tasks.jar {
  dependsOn(tasks.named("shadowJar"))
  archiveClassifier.set("empty")
}
