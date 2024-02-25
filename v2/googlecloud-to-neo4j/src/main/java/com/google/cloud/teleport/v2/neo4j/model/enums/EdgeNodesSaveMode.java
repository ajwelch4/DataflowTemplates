/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.neo4j.model.enums;

public enum EdgeNodesSaveMode {
  create,
  match,
  merge;

  public static EdgeNodesSaveMode defaultFor(SaveMode mode) {
    if (mode == SaveMode.merge) {
      return match;
    }
    if (mode == SaveMode.append) {
      return create;
    }
    throw new IllegalArgumentException(
        String.format(
            "Cannot determine default edge node save mode: unsupported save mode %s", mode.name()));
  }
}
