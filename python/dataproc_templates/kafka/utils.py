# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from typing import Dict
from typing import List

from confluent_kafka.schema_registry import RegisteredSchema
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry import SchemaReference
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType


@udf(returnType=StructType([
    StructField("schema_id", StringType()),
    StructField("schema", StructType([
        StructField("schema_str", StringType()),
        StructField("schema_type", StringType()),
        StructField("references", ArrayType(StructType([
            StructField("name", StringType()),
            StructField("subject", StringType()),
            StructField("version", StringType())
        ])))
    ])),
    StructField("subject", StringType()),
    StructField("version", StringType())
]))
def get_schema_latest_version(subject: str = None, endpoint: str = None,
    api_key: str = None, api_secret: str = None) -> Dict:
    res = None
    try:
        sr_client = SchemaRegistryClient(conf={
            "url": endpoint,
            "basic.auth.user.info": f"{api_key}:{api_secret}"
        })
        lv: RegisteredSchema = sr_client.get_latest_version(subject)
        schema: Schema = lv.schema
        references: List[SchemaReference] = schema.references
        res = {
            "schema_id": lv.schema_id,
            "schema": {
                "schema_str": schema.schema_str,
                "schema_type": schema.schema_type,
                "references": [
                    {
                        "name": r.name,
                        "subject": r.subject,
                        "version": r.version,
                    }
                    for r in references
                ]
            },
            "subject": lv.subject,
            "version": lv.version
        }
        return res
    except Exception as e:
        logging.exception(f"Error getting schema latest version for {subject}")
        raise e
