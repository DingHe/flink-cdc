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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.udf.UserDefinedFunction;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Descriptor of a UDF function. */
// 专门用于管理和描述用户在数据转换（Transform）过程中使用的自定义函数（UDF）。
// 在 Flink CDC 的 Pipeline 定义中，用户可以使用自定义函数来处理字段。该类的主要作用包括：
// 元数据持有者：保存 UDF 的注册名称、全类名（Classpath）、初始化参数等信息。
// 类型兼容性桥梁：Flink CDC 支持两种 UDF：
// CDC Pipeline UDF（实现 CDC 自己的接口）。
// Flink ScalarFunction（传统的 Flink 自定义函数）。 该类负责识别并适配这两者。
// 运行时发现：在算子初始化阶段，利用 Java 反射机制实例化 UDF，并自动探测函数的返回值类型（Return Type）。
@Internal
public class UserDefinedFunctionDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;
    // UDF 在表达式中使用的别名（如 my_func）。
    private final String name;
    // UDF 类的全限定名（如 com.example.MyUpperFunction）
    private final String classpath;
    // 仅包含类名，不包含包路径。由 classpath 截取而来。
    private final String className;
    // 返回值类型提示。对于实现 CDC 接口的 UDF，它会记录函数返回的数据类型（如 STRING, INT 等）。
    private final DataType returnTypeHint;
    // rue: 表示该类实现了 UserDefinedFunction 接口。
    private final boolean isCdcPipelineUdf;
    // parameters (Map<String, String>): 初始化参数。存储在配置中定义的键值对，用于在运行时动态配置 UDF。
    private final Map<String, String> parameters;

    public UserDefinedFunctionDescriptor(String name, String classpath) {
        this(name, classpath, new HashMap<>());
    }

    public UserDefinedFunctionDescriptor(
            Tuple3<String, String, Map<String, String>> descriptorTuple) {
        this(descriptorTuple.f0, descriptorTuple.f1, descriptorTuple.f2);
    }

    public UserDefinedFunctionDescriptor(
            String name, String classpath, Map<String, String> parameters) {
        this.name = name;
        this.parameters = parameters;
        this.classpath = classpath;
        this.className = classpath.substring(classpath.lastIndexOf('.') + 1);
        try {
            Class<?> clazz = Class.forName(classpath);
            isCdcPipelineUdf = isCdcPipelineUdf(clazz);
            if (isCdcPipelineUdf) {
                // We use reflection to invoke UDF methods since we may add more methods
                // into UserDefinedFunction interface, thus the provided UDF classes
                // might not be compatible with the interface definition in CDC common.
                returnTypeHint =
                        (DataType)
                                clazz.getMethod("getReturnType")
                                        .invoke(clazz.getConstructor().newInstance());
            } else {
                returnTypeHint = null;
            }
        } catch (ClassNotFoundException
                | InvocationTargetException
                | IllegalAccessException
                | NoSuchMethodException
                | InstantiationException e) {
            throw new IllegalArgumentException(
                    "Failed to instantiate UDF " + name + "@" + classpath, e);
        }
    }

    private boolean isCdcPipelineUdf(Class<?> clazz) {
        Class<?> cdcPipelineUdfClazz = UserDefinedFunction.class;
        Class<?> flinkScalarFunctionClazz = org.apache.flink.table.functions.ScalarFunction.class;

        if (Arrays.stream(clazz.getInterfaces())
                .map(Class::getName)
                .collect(Collectors.toList())
                .contains(cdcPipelineUdfClazz.getName())) {
            return true;
        } else if (clazz.getSuperclass().getName().equals(flinkScalarFunctionClazz.getName())) {
            return false;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Failed to detect UDF class "
                                    + clazz
                                    + " since it never implements %s or extends Flink %s.",
                            cdcPipelineUdfClazz,
                            flinkScalarFunctionClazz));
        }
    }

    public DataType getReturnTypeHint() {
        return returnTypeHint;
    }

    public boolean isCdcPipelineUdf() {
        return isCdcPipelineUdf;
    }

    public String getName() {
        return name;
    }

    public String getClasspath() {
        return classpath;
    }

    public String getClassName() {
        return className;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UserDefinedFunctionDescriptor that = (UserDefinedFunctionDescriptor) o;
        return isCdcPipelineUdf == that.isCdcPipelineUdf
                && Objects.equals(name, that.name)
                && Objects.equals(classpath, that.classpath)
                && Objects.equals(className, that.className)
                && Objects.equals(returnTypeHint, that.returnTypeHint)
                && Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                name, classpath, className, returnTypeHint, isCdcPipelineUdf, parameters);
    }

    @Override
    public String toString() {
        return "UserDefinedFunctionDescriptor{"
                + "name='"
                + name
                + '\''
                + ", classpath='"
                + classpath
                + '\''
                + ", className='"
                + className
                + '\''
                + ", returnTypeHint="
                + returnTypeHint
                + ", isCdcPipelineUdf="
                + isCdcPipelineUdf
                + ", parameters="
                + parameters
                + '}';
    }
}
