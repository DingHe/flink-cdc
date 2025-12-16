/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.composer.definition;

import java.util.Objects;

/**
 * Definition of a user-defined function.
 *
 * <p>A transformation definition contains:
 *
 * <ul>
 *   <li>name: Static method name of user-defined functions.
 *   <li>classpath: Fully-qualified class path of package containing given function.
 * </ul>
 */
// UdfDef 类的核心作用是以结构化的方式表示 Flink CDC 流水线中需要注册和使用的自定义函数。
// 在 Flink CDC 流水线中，UDF 允许用户在转换（Transformation）步骤中执行自定义的业务逻辑。
// UdfDef 存储了查找和调用该函数所需的基本信息。
// 概括来说，它定义了“自定义函数的名称和位置”：
// 名称 (Name): 函数在 Flink 运行时环境中注册和被调用的静态方法名称。
// 类路径 (Classpath): 包含该函数的类的完全限定名（Fully-qualified class path）。
public class UdfDef {
    // 用户自定义函数的静态方法名。
    // 这是在 Flink SQL 或转换表达式中引用该函数时使用的名称。
    private final String name;
    // 包含该函数的类的完全限定名。
    // 必需，指定了该函数所在的 Java 类的包路径和类名（例如 com.example.MyFunctions）。这是 Flink 运行时加载和查找该函数的依据。
    private final String classpath;

    public UdfDef(String name, String classpath) {
        this.name = name;
        this.classpath = classpath;
    }

    public String getName() {
        return name;
    }

    public String getClasspath() {
        return classpath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UdfDef udfDef = (UdfDef) o;
        return Objects.equals(name, udfDef.name) && Objects.equals(classpath, udfDef.classpath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, classpath);
    }

    @Override
    public String toString() {
        return "UdfDef{" + "name='" + name + '\'' + ", classpath='" + classpath + '\'' + '}';
    }
}
