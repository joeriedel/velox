/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
#include "velox/experimental/cudf/CudfConfig.h"
#include "velox/experimental/cudf/exec/CudfBatchConcat.h"
#include "velox/experimental/cudf/exec/ToCudf.h"
#include "velox/experimental/cudf/tests/CudfFunctionBaseTest.h"

#include "velox/exec/FilterProject.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

#include <algorithm>
#include <string_view>

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace {

constexpr std::string_view kPreCudfAdapterName = "pre-cuDF-test";

bool isIdentityProject(const core::ProjectNode& project) {
  const auto& sourceType = project.sources()[0]->outputType();
  if (project.projections().size() != sourceType->size() ||
      project.names().size() != sourceType->size()) {
    return false;
  }

  for (auto i = 0; i < sourceType->size(); ++i) {
    const auto field =
        std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
            project.projections()[i]);
    if (!field || !field->isInputColumn() ||
        field->name() != sourceType->nameOf(i) ||
        project.names()[i] != sourceType->nameOf(i)) {
      return false;
    }
  }
  return true;
}

bool replaceIdentityProjectWithCudfBatchConcat(
    const DriverFactory& factory,
    Driver& driver) {
  const auto operators = driver.operators();
  for (auto index = 0; index < operators.size(); ++index) {
    const auto* filterProject =
        dynamic_cast<const FilterProject*>(operators[index]);
    if (!filterProject) {
      continue;
    }

    const auto planNodeIt = std::find_if(
        factory.planNodes.begin(),
        factory.planNodes.end(),
        [&](const auto& node) {
          return node->id() == filterProject->planNodeId();
        });
    if (planNodeIt == factory.planNodes.end()) {
      continue;
    }

    const auto project =
        std::dynamic_pointer_cast<const core::ProjectNode>(*planNodeIt);
    if (!project || !isIdentityProject(*project)) {
      continue;
    }

    std::vector<std::unique_ptr<Operator>> replacement;
    replacement.push_back(
        std::make_unique<cudf_velox::CudfBatchConcat>(
            filterProject->operatorId(), driver.driverCtx(), project));
    factory.replaceOperators(driver, index, index + 1, std::move(replacement));
  }

  // Continue to the cuDF adapter so it can add conversion boundaries.
  return false;
}

void registerPreCudfAdapter() {
  DriverFactory::registerAdapter(
      {std::string{kPreCudfAdapterName},
       {},
       replaceIdentityProjectWithCudfBatchConcat});
}

void unregisterPreCudfAdapter() {
  auto& adapters = DriverFactory::adapters;
  adapters.erase(
      std::remove_if(
          adapters.begin(),
          adapters.end(),
          [](const DriverAdapter& adapter) {
            return adapter.label == kPreCudfAdapterName;
          }),
      adapters.end());
}

} // namespace

class AdapterOperatorTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    cudf_velox::CudfConfig::getInstance().allowCpuFallback = false;
    registerPreCudfAdapter();
    cudf_velox::registerCudf();
  }

  void TearDown() override {
    cudf_velox::unregisterCudf();
    unregisterPreCudfAdapter();
    OperatorTestBase::TearDown();
  }
};

TEST_F(AdapterOperatorTest, adapterStatsMergedIntoPlanNode) {
  auto data = makeRowVector({"c0"}, {makeFlatVector<int32_t>({1, 2, 3, 4, 5})});

  core::PlanNodeId projNodeId;
  auto plan = PlanBuilder()
                  .values({data})
                  .project({"c0 * 2 as x"})
                  .capturePlanNodeId(projNodeId)
                  .planNode();

  std::shared_ptr<exec::Task> task;
  AssertQueryBuilder(plan).copyResults(pool(), task);

  auto stats = toPlanStats(task->taskStats());
  auto& projStats = stats.at(projNodeId);

  EXPECT_TRUE(projStats.isMultiOperatorTypeNode());
  EXPECT_TRUE(projStats.operatorStats.count("CudfToVelox"));
}

TEST_F(AdapterOperatorTest, adaptsCudfOperatorInsertedByEarlierAdapter) {
  auto data = makeRowVector(
      {"c0", "c1"},
      {makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
       makeFlatVector<int64_t>({10, 20, 30, 40, 50})});

  core::PlanNodeId projNodeId;
  auto plan = PlanBuilder()
                  .values({data})
                  .project({"c0", "c1"})
                  .capturePlanNodeId(projNodeId)
                  .planNode();

  std::shared_ptr<Task> task;
  auto result = AssertQueryBuilder(plan).copyResults(pool(), task);
  facebook::velox::test::assertEqualVectors(data, result);

  auto stats = toPlanStats(task->taskStats());
  const auto& projStats = stats.at(projNodeId);
  EXPECT_EQ(projStats.operatorStats.size(), 3);
  EXPECT_TRUE(projStats.operatorStats.count("CudfFromVelox"));
  EXPECT_TRUE(projStats.operatorStats.count("CudfBatchConcat"));
  EXPECT_TRUE(projStats.operatorStats.count("CudfToVelox"));
  EXPECT_FALSE(projStats.operatorStats.count("FilterProject"));
}
