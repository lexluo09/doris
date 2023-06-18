// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "hudi_jni_reader.h"

#include <map>
#include <ostream>

#include "runtime/descriptors.h"
#include "runtime/types.h"
#include "vec/core/types.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

const std::string HudiJniReader::HADOOP_FS_PREFIX = "hadoop_fs.";

HudiJniReader::HudiJniReader(const TFileScanRangeParams& scan_params,
                             const THudiFileDesc& hudi_params,
                             const std::vector<SlotDescriptor*>& file_slot_descs,
                             RuntimeState* state, RuntimeProfile* profile)
        : _scan_params(scan_params),
          _hudi_params(hudi_params),
          _file_slot_descs(file_slot_descs),
          _state(state),
          _profile(profile) {
    std::vector<std::string> required_fields;
    for (auto& desc : _file_slot_descs) {
        required_fields.emplace_back(desc->col_name());
    }

    std::map<String, String> params = {
            {"base_path", _hudi_params.base_path},
            {"data_file_path", _hudi_params.data_file_path},
            {"data_file_length", std::to_string(_hudi_params.data_file_length)},
            {"delta_file_paths", join(_hudi_params.delta_logs, ",")},
            {"hudi_column_names", join(_hudi_params.column_names, ",")},
            {"hudi_column_types", join(_hudi_params.column_types, "#")},
            {"required_fields", join(required_fields, ",")},
            {"instant_time", _hudi_params.instant_time},
            {"serde", _hudi_params.serde},
            {"input_format", _hudi_params.input_format}};

    // Use compatible hadoop client to read data
    for (auto& kv : _scan_params.properties) {
        params[HADOOP_FS_PREFIX + kv.first] = kv.second;
    }

    jclass scanner_loader_class = _jni_env->FindClass("org/apache/doris/hudi/HudiScannerLoader");
    jmethodID scanner_loader_constructor = _jni_env->GetMethodID(scanner_loader_class, "<init>", "()V");
    jobject scanner_loader_obj = _jni_env->NewObject(scanner_loader_class, scanner_loader_constructor);
    jmethodID get_loader_method =
            _jni_env->GetMethodID(scanner_loader_class, "getLoaderClass", "()Ljava/lang/Class;");
    _jni_scanner_cls = (jclass)_jni_env->CallObjectMethod(scanner_loader_obj, get_loader_method);

    _jni_env->DeleteLocalRef(scanner_loader_class);
    _jni_env->DeleteLocalRef(scanner_loader_obj);

    jmethodID scanner_constructor = _jni_env->GetMethodID(_jni_scanner_cls, "<init>", "(ILjava/util/Map;)V");

    jclass hashmap_class = _jni_env->FindClass("java/util/HashMap");
    jmethodID hashmap_constructor = _jni_env->GetMethodID(hashmap_class, "<init>", "(I)V");
    jobject hashmap_object = _jni_env->NewObject(hashmap_class, hashmap_constructor, params.size());
    jmethodID hashmap_put =
            _jni_env->GetMethodID(hashmap_class, "put", "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;");

    for (const auto& it : params) {
        jstring key = _jni_env->NewStringUTF(it.first.c_str());
        jstring value = _jni_env->NewStringUTF(it.second.c_str());

        _jni_env->CallObjectMethod(hashmap_object, hashmap_put, key, value);
        _jni_env->DeleteLocalRef(key);
        _jni_env->DeleteLocalRef(value);
    }
    _jni_env->DeleteLocalRef(hashmap_class);

    _jni_connector = _jni_env->NewObject(_jni_scanner_cls, scanner_constructor, hashmap_object);
    _jni_env->DeleteLocalRef(hashmap_object);
}

Status HudiJniReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    RETURN_IF_ERROR(_jni_connector->get_nex_block(block, read_rows, eof));
    if (*eof) {
        RETURN_IF_ERROR(_jni_connector->close());
    }
    return Status::OK();
}

Status HudiJniReader::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                                  std::unordered_set<std::string>* missing_cols) {
    for (auto& desc : _file_slot_descs) {
        name_to_type->emplace(desc->col_name(), desc->type());
    }
    return Status::OK();
}

Status HudiJniReader::init_reader(
        std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range) {
    _colname_to_value_range = colname_to_value_range;
    RETURN_IF_ERROR(_jni_connector->init(colname_to_value_range));
    return _jni_connector->open(_state, _profile);
}
} // namespace doris::vectorized
