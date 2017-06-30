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
package org.apache.spark.deploy.kubernetes.submit.submitsteps

import org.apache.spark.internal.Logging

import io.fabric8.kubernetes.api.model.ContainerBuilder
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.deploy.kubernetes.submit.KubernetesFileUtils


private[spark] class PythonStep(
    primaryPyFile: String,
    otherPyFiles: Seq[String],
    appArgs: Array[String],
    filesDownloadPath: String) extends KubernetesSubmissionStep {

  override def prepareSubmission(driverSpec: KubernetesDriverSpec): KubernetesDriverSpec = {
    val arguments : Array[String] = otherPyFiles.toList match {
      case Nil => null +: appArgs
      case a :: b => a match {
        case _ if a == "" && b == Nil => null +: appArgs
        case _ => appArgs
      }
    }
    val withPythonPrimaryFileContainer = new ContainerBuilder(driverSpec.driverContainer)
      .addNewEnv()
        .withName(ENV_DRIVER_ARGS)
        .withValue(arguments.mkString(" "))
        .endEnv()
      .addNewEnv()
        .withName(ENV_PYSPARK_PRIMARY)
        .withValue(KubernetesFileUtils.resolveFilePath(primaryPyFile, filesDownloadPath))
        .endEnv()
      .addNewEnv()
        .withName(ENV_PYSPARK_FILES)
        .withValue(
            KubernetesFileUtils.resolveFilePaths(otherPyFiles, filesDownloadPath).mkString(","))
        .endEnv()
    driverSpec.copy(driverContainer = withPythonPrimaryFileContainer.build())
  }
}
