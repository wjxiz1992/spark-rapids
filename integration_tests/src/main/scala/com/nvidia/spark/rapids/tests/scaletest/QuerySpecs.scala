/*
 * Copyright (c) 2023, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.tests.scaletest

import com.nvidia.spark.rapids.tests.scaletest.ScaleTest.Config

import org.apache.spark.sql.SparkSession

case class TestQuery(name: String, content: String, iterations: Int, timeout: Long,
  description: String)

class QuerySpecs(config: Config, spark: SparkSession) {
  private val baseInputPath = s"${config.inputDir}/" +
    "SCALE_" +
    s"${config.scaleFactor}_" +
    s"${config.complexity}_" +
    s"${config.format}_" +
    s"${config.version}_" +
    s"${config.seed}"
  private val tables = Seq("a_facts",
    "b_data",
    "c_data",
    "d_data",
    "e_data",
    "f_facts",
    "g_data")

  /**
   *
   * @param prefix     column name prefix e.g. "b_data"
   * @param startIndex start column index e.g. 1
   * @param endIndex   end column index e.g. 10
   * @return an expanded string for all columns with comma separated
   */
  private def expandDataColumnWithRange(prefix: String, startIndex: Int, endIndex: Int): String = {
    (startIndex to endIndex).map(i => s"${prefix}_$i").mkString(",")
  }

  /**
   * expand the key column names by complexity. e.g. c_key2_* when complexity is 5:
   * "c_key2_1, c_key2_2, c_key2_3, c_key2_4, c_key2_5"
   * @param prefix column name prefix
   * @param complexity indicates the number of columns
   * @return
   */
  private def expandKeyColumnByComplexity(prefix: String, complexity: Int): String = {
    (1 to complexity).map(i => s"${prefix}_$i").mkString(",")
  }

  /**
   * expand the columns for aggregations functions by complexity. e.g. MIN(b_data_*) when
   * complexity is 3:
   * "MIN(b_data_1), MIN(b_data_2), MIN(b_data_3), MIN(b_data_4), MIN(b_data_5)"
   * @param prefix column name prefix
   * @param aggFunc aggregate function name
   * @param complexity indicates the number of columns
   * @return
   */
  private def expandAggColumnByComplexity(prefix: String, aggFunc: String, complexity: Int): String
  = {
    (1 to complexity).map(i => s"${aggFunc}(${prefix}_$i)").mkString(",")
  }

  /**
   * expand the where clause with AND condition after join by complexity. e.g. c_key2_* = d_key2_*
   * with complexity 3:
   * "c_key2_1 = d_key2_1 AND c_key2_2 = d_key2_2 AND c_key2_3 = d_key2_3"
   * @param prefix
   * @param complexity
   * @return
   */
  private def expandWhereClauseWithAndByComplexity(lhsPrefix: String, rhsPrefix: String,
    complexity: Int): String = {
    (1 to complexity).map(i => s"${lhsPrefix}_$i = ${rhsPrefix}_i").mkString(" AND ")
  }

  /**
   * Read data and initialize temp views in Spark
   */
  def initViews(): Unit = {

    for (table <- tables) {
      spark.read.format(config.format).load(s"$baseInputPath/$table")
        .createOrReplaceTempView(table)
    }
  }

  def getCandidateQueries(): Map[String, TestQuery] = {
    /*
    All queries are defined here
     */
    val allQueries = Map[String, TestQuery](
      "q1" -> TestQuery("q1",
        "SELECT a_facts.*, " +
          expandDataColumnWithRange("b_data", 1, 10) +
          " FROM b_data JOIN a_facts WHERE " +
          "primary_a = b_foreign_a",
        config.iterations,
        config.timeout,
        "Inner join with lots of ride along columns"),

      "q2" -> TestQuery("q2",
        "SELECT a_facts.*," +
          expandDataColumnWithRange("b_data", 1, 10) +
          " FROM b_data FULL OUTER JOIN a_facts WHERE primary_a = b_foreign_a",
        config.iterations,
        config.timeout,
        "Full outer join with lots of ride along columns"),

      "q3" -> TestQuery("q3",
        "SELECT a_facts.*," +
          expandDataColumnWithRange("b_data", 1, 10) +
          " FROM b_data LEFT OUTER JOIN a_facts WHERE primary_a = b_foreign_a",
        config.iterations,
        config.timeout,
        "Left outer join with lots of ride along columns"),

      "q4" -> TestQuery("q4",
        "SELECT c_data.* FROM c_data LEFT ANTI JOIN a_facts WHERE primary_a = c_foreign_a",
        config.iterations,
        config.timeout,
        "Left anti-join lots of ride along columns."),
      "q5" -> TestQuery("q5",
        "SELECT c_data.* FROM c_data LEFT SEMI JOIN a_facts WHERE primary_a = c_foreign_a",
        config.iterations,
        config.timeout,
        "Left semi-join lots of ride along columns."),
      "q6" -> TestQuery("q6",
        s"SELECT " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}, COUNT(1), " +
          s"${expandAggColumnByComplexity("c_data", "MIN", config.complexity)}," +
          s"${expandAggColumnByComplexity("d_data", "MAX", config.complexity)} " +
          s"FROM c_data JOIN d_data WHERE " +
          s"${expandWhereClauseWithAndByComplexity(
            "c_key2", "d_key2", config.complexity)} GROUP BY " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}",
        config.iterations,
        config.timeout,
        "Exploding inner large key count equi-join followed by min/max agg."),
      "q7" -> TestQuery("q7",
        s"SELECT " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}, COUNT(1), " +
          s"${expandAggColumnByComplexity("c_data", "MIN", config.complexity)}," +
          s"${expandAggColumnByComplexity("d_data", "MAX", config.complexity)} " +
          s"FROM c_data FULL OUTER JOIN d_data WHERE " +
          s"${expandWhereClauseWithAndByComplexity(
              "c_key2", "d_key2", config.complexity)} GROUP BY " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}",
        config.iterations,
        config.timeout,
        "Exploding full outer large key count equi-join followed by min/max agg."),
      "q8" -> TestQuery("q8",
        s"SELECT " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}, COUNT(1), " +
          s"${expandAggColumnByComplexity("c_data", "MIN", config.complexity)}," +
          s"${expandAggColumnByComplexity("d_data", "MAX", config.complexity)} " +
          s"FROM c_data LEFT OUTER JOIN d_data WHERE " +
          s"${expandWhereClauseWithAndByComplexity(
              "c_key2", "d_key2", config.complexity)} " +
          s"GROUP BY " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}",
        config.iterations,
        config.timeout,
        "Exploding left outer large key count equi-join followed by min/max agg."),
      "q9" -> TestQuery("q9",
        s"SELECT " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}, " +
          s"COUNT(1), " +
          s"${expandAggColumnByComplexity("MIN", "c_data", config.complexity)} " +
          s"FROM c_data LEFT SEMI JOIN d_data WHERE " +
          s"${expandWhereClauseWithAndByComplexity(
              "c_key2", "d_key2", config.complexity)}" +
          s" GROUP BY " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}",
        config.iterations,
        config.timeout,
      "Left semi large key count equi-join followed by min/max agg."),
      "q10" -> TestQuery("q10",
        s"SELECT " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}," +
          s" COUNT(1)," +
          s" ${expandAggColumnByComplexity("c_data", "MIN", config.complexity)}" +
          s" FROM c_data LEFT ANTI JOIN d_data WHERE " +
          s"${expandWhereClauseWithAndByComplexity(
              "c_key2", "d_key2", config.complexity)} " +
          s"GROUP BY " +
          s"${expandKeyColumnByComplexity("c_key2", config.complexity)}",
        config.iterations,
        config.timeout,
      "Left anti large key count equi-join followed by min/max agg."),

    )
    if (config.queries.isEmpty) {
      allQueries
    } else {
      config.queries.map(q => {
        (q, allQueries(q))
      }).toMap
    }
  }
}
