/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that eliminates aggregate functions of GROUP BY keys.
 *
 * <p>For example,
 * {@code SELECT sal, max(sal) FROM emp GROUP BY sal}
 * can be simplified to
 * {@code SELECT sal, sal FROM emp GROUP BY sal}.
 *
 * <p>Currently supports the following aggregate functions when their
 * arguments exist in the aggregate's group set or are deterministic
 * expressions involving only group set columns and constants:
 * <ul>
 *   <li>{@code MAX}</li>
 *   <li>{@code MIN}</li>
 *   <li>{@code AVG}</li>
 *   <li>{@code ANY_VALUE}</li>
 * </ul>
 *
 * @see CoreRules#AGGREGATE_REDUCE_FUNCTIONS_ON_GROUP_KEYS
 */
@Value.Enclosing
public class AggregateReduceFunctionsOnGroupKeysRule
    extends RelRule<AggregateReduceFunctionsOnGroupKeysRule.Config>
    implements TransformationRule {

  /** Creates an AggregateReduceFunctionsOnGroupKeysRule. */
  protected AggregateReduceFunctionsOnGroupKeysRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final List<AggregateCall> oldCalls = aggregate.getAggCallList();
    final int groupCount = aggregate.getGroupCount();
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();

    final List<AggregateCall> newCalls = new ArrayList<>();
    final List<RexNode> projects = new ArrayList<>();

    // Pass through group keys.
    for (int i = 0; i < groupCount; i++) {
      projects.add(rexBuilder.makeInputRef(aggregate, i));
    }

    boolean changed = false;
    int newCallOrdinal = 0;
    for (AggregateCall oldCall : oldCalls) {
      final @Nullable RexNode reduced = reduce(aggregate, oldCall, rexBuilder);
      if (reduced != null) {
        projects.add(reduced);
        changed = true;
      } else {
        newCalls.add(oldCall);
        projects.add(
            rexBuilder.makeInputRef(
                oldCall.getType(), groupCount + newCallOrdinal));
        newCallOrdinal++;
      }
    }

    if (!changed) {
      return;
    }

    final RelNode newAggregate =
        aggregate.copy(
            aggregate.getTraitSet(),
            aggregate.getInput(),
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            newCalls);
    relBuilder.push(newAggregate);
    relBuilder.project(projects);
    call.transformTo(relBuilder.build());
  }

  /**
   * Tries to reduce an aggregate call to a reference to a group-by key
   * or to an expression involving only group-by keys and constants.
   *
   * @return the reduced expression, or null if cannot reduce
   */
  private static @Nullable RexNode reduce(
      Aggregate aggregate,
      AggregateCall call,
      RexBuilder rexBuilder) {
    if (!Aggregate.isSimple(aggregate)) {
      return null;
    }
    if (call.hasFilter()
        || call.distinctKeys != null
        || call.collation != RelCollations.EMPTY) {
      return null;
    }
    final SqlKind kind = call.getAggregation().getKind();
    switch (kind) {
    case AVG:
    case MAX:
    case MIN:
    case ANY_VALUE:
      break;
    default:
      return null;
    }
    final List<Integer> argList = call.getArgList();
    if (argList.size() != 1) {
      return null;
    }
    final int arg = argList.get(0);

    // Case 1: argument directly references a group-by key
    if (aggregate.getGroupSet().get(arg)) {
      final int groupIndex = aggregate.getGroupSet().asList().indexOf(arg);
      RexNode ref = RexInputRef.of(groupIndex, aggregate.getRowType().getFieldList());
      if (!ref.getType().equals(call.getType())) {
        ref = rexBuilder.makeCast(call.getParserPosition(), call.getType(), ref);
      }
      return ref;
    }

    // Case 2: argument is an expression in a Project below the Aggregate
    RelNode input = aggregate.getInput();
    if (input instanceof HepRelVertex) {
      input = ((HepRelVertex) input).getCurrentRel();
    }
    if (!(input instanceof Project)) {
      return null;
    }
    final Project project = (Project) input;
    if (arg < 0 || arg >= project.getProjects().size()) {
      return null;
    }
    final RexNode expr = project.getProjects().get(arg);
    if (!RexUtil.isDeterministic(expr)) {
      return null;
    }
    final @Nullable RexNode translated =
        translateToGroupRefs(expr, project, aggregate);
    if (translated == null) {
      return null;
    }
    if (!translated.getType().equals(call.getType())) {
      return rexBuilder.makeCast(call.getParserPosition(), call.getType(), translated);
    }
    return translated;
  }

  /**
   * Translates an expression so that its {@link RexInputRef}s reference
   * the group keys of the aggregate rather than the input to the project.
   *
   * @return the translated expression, or null if the expression references
   * columns that are not group-by keys
   */
  private static @Nullable RexNode translateToGroupRefs(
      RexNode expr, Project project, Aggregate aggregate) {
    final List<RexNode> projects = project.getProjects();
    final GroupRefTranslator translator = new GroupRefTranslator(projects, aggregate);
    final RexNode result = expr.accept(translator);
    return translator.failed ? null : result;
  }

  /** Shuttle that translates input refs to aggregate group key refs. */
  private static class GroupRefTranslator extends RexShuttle {
    private final List<RexNode> projects;
    private final Aggregate aggregate;
    private boolean failed = false;

    GroupRefTranslator(List<RexNode> projects, Aggregate aggregate) {
      this.projects = projects;
      this.aggregate = aggregate;
    }

    @Override public RexNode visitInputRef(RexInputRef inputRef) {
      if (failed) {
        return inputRef;
      }
      final int inputIndex = inputRef.getIndex();
      int projectOutputIndex = -1;
      for (int i = 0; i < projects.size(); i++) {
        final RexNode projExpr = projects.get(i);
        if (projExpr instanceof RexInputRef
            && ((RexInputRef) projExpr).getIndex() == inputIndex) {
          projectOutputIndex = i;
          break;
        }
      }
      if (projectOutputIndex < 0
          || !aggregate.getGroupSet().get(projectOutputIndex)) {
        failed = true;
        return inputRef;
      }
      final int groupIndex =
          aggregate.getGroupSet().asList().indexOf(projectOutputIndex);
      return RexInputRef.of(groupIndex, aggregate.getRowType().getFieldList());
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableAggregateReduceFunctionsOnGroupKeysRule.Config.of()
        .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
        .withOperandFor(LogicalAggregate.class);

    @Override default AggregateReduceFunctionsOnGroupKeysRule toRule() {
      return new AggregateReduceFunctionsOnGroupKeysRule(this);
    }

    /** Defines an operand tree for the given class. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass) {
      return withOperandSupplier(b ->
          b.operand(aggregateClass)
              .predicate(Aggregate::isSimple)
              .anyInputs())
          .as(Config.class);
    }
  }
}
