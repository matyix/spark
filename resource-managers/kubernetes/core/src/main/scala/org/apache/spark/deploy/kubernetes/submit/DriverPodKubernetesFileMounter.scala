/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.kubernetes.submit

import io.fabric8.kubernetes.api.model.{Container, PodBuilder}

import org.apache.spark.deploy.kubernetes.constants._

 /**
  * Trait that is responsible for providing full file-paths dynamically after
  * the filesDownloadPath has been defined. The file-names are then stored in the
  * environmental variables in the driver-pod.
  */
private[spark] trait DriverPodKubernetesFileMounter {
  def addPySparkFiles(mainAppResource: String, pythonFiles: List[String],
    mainContainerName: String, originalPodSpec: PodBuilder) : PodBuilder
}

private[spark] class DriverPodKubernetesFileMounterImpl(filesDownloadPath: String)
  extends DriverPodKubernetesFileMounter {
  val LocalPattern = "(local://)(.*)".r
  val FilePattern = "(file:/)(.*)".r
  def getName(file: String, separatorChar: Char) : String = {
    val index: Int = file.lastIndexOf(separatorChar)
    file.substring(index + 1)
  }
  def fileLoc(file: String) : String = file match {
    case "" => ""
    case LocalPattern(_, file_name) => file_name
    case FilePattern(_, file_name) => filesDownloadPath + "/" + getName(file_name, '/')
    case _ => filesDownloadPath + "/" + getName(file, '/')
  }
  def pythonFileLocations(pFiles: List[String], mainAppResource: String) : String = {
    def recFileLoc(file: List[String]): List[String] = file match {
      case Nil => List.empty[String]
      case a::b => a match {
        case _ if a==mainAppResource => recFileLoc(b)
        case _ => fileLoc(a) +: recFileLoc(b)
      }
  }
    recFileLoc(pFiles).mkString(",")
  }
  override def addPySparkFiles(mainAppResource: String, pythonFiles: List[String],
                               mainContainerName: String,
                               originalPodSpec: PodBuilder): PodBuilder = {
    originalPodSpec
      .editSpec()
      .editMatchingContainer(new ContainerNameEqualityPredicate(mainContainerName))
      .addNewEnv()
      .withName(ENV_PYSPARK_PRIMARY)
      .withValue(fileLoc(mainAppResource))
      .endEnv()
      .addNewEnv()
      .withName(ENV_PYSPARK_FILES)
      .withValue(pythonFileLocations(pythonFiles, mainAppResource))
      .endEnv()
      .endContainer()
      .endSpec()
  }
}
