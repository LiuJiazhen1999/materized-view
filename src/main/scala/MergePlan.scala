import org.apache.spark.sql.catalyst.catalog.CatalogRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId, NamedExpression, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, GlobalLimit, Join, LocalLimit, LogicalPlan, Project, Sort, SubqueryAlias}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Alias

case class MergePlan() {
  var catalogRelationMap = new scala.collection.mutable.HashMap[String, ExprId]

  def mergeMultiPlans(plans: Seq[LogicalPlan]): LogicalPlan = {
    plans.apply(0)
  }

  def prePostProcess(originplan1: LogicalPlan, originplan2: LogicalPlan): LogicalPlan = {
    val plan1 = originplan1
    val plan2 = originplan2
    catalogRelationMap.clear()
    val exprIdPrefix = "ffaabcde"
    var aliasMap = new scala.collection.mutable.HashMap[Long, Long]
    var maxExprId = 0L
    var transPlan1 = plan1 transformUp {
      case catalogRelation: CatalogRelation => {
        val output1 = catalogRelation.output
        for(value <- output1) {
          catalogRelationMap.put(value.qualifier.getOrElse("")+value.name, value.exprId)
          maxExprId = if (maxExprId > value.exprId.id) maxExprId else value.exprId.id
        }
        catalogRelation
      }
      case plan => {
        plan transformExpressions {
          case alias: Alias => {
            maxExprId += 1
            aliasMap.put(alias.exprId.id, maxExprId)
            alias.copy(name = exprIdPrefix+(maxExprId))(exprId = alias.exprId.copy(id = maxExprId), qualifier = alias.qualifier, explicitMetadata = alias.explicitMetadata, isGenerated = alias.isGenerated)
          }
          case attributeReference: AttributeReference =>
            if (aliasMap.contains(attributeReference.exprId.id)) {
              attributeReference.withName(exprIdPrefix + aliasMap.get(attributeReference.exprId.id)).withExprId(attributeReference.exprId.copy(id = aliasMap.get(attributeReference.exprId.id).getOrElse(attributeReference.exprId.id)))
            } else {
              attributeReference
            }
        }
      }
    }
    val transPlan2 = plan2 transformUp {
      case plan => {
        plan transformExpressions {
          case alias: Alias => {
            maxExprId += 1
            aliasMap.put(alias.exprId.id, maxExprId)
            alias.copy(name = exprIdPrefix+(maxExprId))(exprId = alias.exprId.copy(id = maxExprId), qualifier = alias.qualifier, explicitMetadata = alias.explicitMetadata, isGenerated = alias.isGenerated)
          }
          case attributeReference: AttributeReference => {
            if (aliasMap.contains(attributeReference.exprId.id)) {
              attributeReference.withName(exprIdPrefix + aliasMap.get(attributeReference.exprId.id)).withExprId(attributeReference.exprId.copy(id = aliasMap.get(attributeReference.exprId.id).getOrElse(attributeReference.exprId.id)))
            } else {
              attributeReference.withExprId(catalogRelationMap.get(attributeReference.qualifier.getOrElse("") + attributeReference.name).getOrElse(attributeReference.exprId))
            }
          }
        }
      }
    }
    mergeTwoPlans(plan1 = transPlan1, plan2 = transPlan2)
  }

  def mergeTwoPlans(plan1: LogicalPlan, plan2: LogicalPlan): LogicalPlan = {
    if (plan1.getClass == classOf[Aggregate] && plan2.getClass == classOf[Aggregate]) {
      val aggregate1 = plan1.asInstanceOf[Aggregate]
      val aggregate2 = plan2.asInstanceOf[Aggregate]
      val aggregateList = Seq.concat(aggregate1.aggregateExpressions, aggregate2.aggregateExpressions).distinct
      aggregate1.copy(aggregateExpressions = aggregateList, child = mergeTwoPlans(aggregate1.child, aggregate2.child))
    } else if (plan1.getClass == classOf[Project] && plan2.getClass == classOf[Project]) {
      val project1 = plan1.asInstanceOf[Project]
      val project2 = plan2.asInstanceOf[Project]
      val projectSeq = Seq.concat(project1.projectList, project2.projectList)
      project1.copy(projectList = projectSeq, child = mergeTwoPlans(project1.child, project2.child))
    } else if (plan1.getClass == classOf[Join] && plan2.getClass == classOf[Join]) {
      //join，个人认为应该condition一样，type一样
      val join1 = plan1.asInstanceOf[Join]
      val join2 = plan2.asInstanceOf[Join]
      join1.copy(left = mergeTwoPlans(join1.left, join2.left), right = mergeTwoPlans(join1.right, join2.right))
    } else if (plan1.getClass == classOf[Filter] || plan2.getClass == classOf[Filter]) {//由于filter不算哈希，所以可能部分语句含有filter算子，部分不含有
      if(plan1.getClass == classOf[Filter] && plan2.getClass == classOf[Filter]) {//如果都等于filter，用一个or链接
        val filter1 = plan1.asInstanceOf[Filter]
        val filter2 = plan2.asInstanceOf[Filter]
        val orCondition = Or(left = filter1.condition, right = filter2.condition)
        filter1.copy(child = mergeTwoPlans(filter1.child, filter2.child), condition = orCondition)
      } else if(plan1.getClass == classOf[Filter]) {//如果只有一个等一filter，那么保留当前filter，另一个留在原地
        val filter1 = plan1.asInstanceOf[Filter]
        filter1.copy(child = mergeTwoPlans(filter1.child, plan2), condition = filter1.condition)
      } else {
        val filter2 = plan2.asInstanceOf[Filter]
        filter2.copy(child = mergeTwoPlans(plan1, filter2.child), condition = filter2.condition)
      }
    } else if (plan1.getClass == classOf[SubqueryAlias] || plan2.getClass == classOf[SubqueryAlias]) {//SubqueryAlias算子，假设是不算哈希的
      if(plan1.getClass == classOf[SubqueryAlias] && plan2.getClass == classOf[SubqueryAlias]) {//当两个都是subqueryalias，随便选择一个作为别名
        val subqueryAlias1 = plan1.asInstanceOf[SubqueryAlias]
        val subqueryAlias2 = plan2.asInstanceOf[SubqueryAlias]
        subqueryAlias1.copy(alias = subqueryAlias1.alias, child = mergeTwoPlans(subqueryAlias1.child, subqueryAlias2.child))
      } else if(plan1.getClass == classOf[SubqueryAlias]) {
        val subqueryAlias1 = plan1.asInstanceOf[SubqueryAlias]
        subqueryAlias1.copy(child = mergeTwoPlans(subqueryAlias1.child, plan2))
      } else {
        val subqueryAlias2 = plan2.asInstanceOf[SubqueryAlias]
        subqueryAlias2.copy(child = mergeTwoPlans(subqueryAlias2.child, plan1))
      }
    } else if (plan1.isInstanceOf[CatalogRelation] && plan2.isInstanceOf[CatalogRelation]) {
      plan1
    } else if(plan1.isInstanceOf[GlobalLimit] || plan2.isInstanceOf[GlobalLimit]) {//放开限制，跳过GlobalLimit
      if(plan1.isInstanceOf[GlobalLimit] && plan2.isInstanceOf[GlobalLimit]) {
        val globalLimit1 = plan1.asInstanceOf[GlobalLimit]
        val globalLimit2 = plan2.asInstanceOf[GlobalLimit]
        mergeTwoPlans(globalLimit1.child, globalLimit2.child)
      } else if(plan1.isInstanceOf[GlobalLimit]) {
        val globalLimit1 = plan1.asInstanceOf[GlobalLimit]
        mergeTwoPlans(globalLimit1.child, plan2)
      } else {
        val globalLimit2 = plan2.asInstanceOf[GlobalLimit]
        mergeTwoPlans(plan1, globalLimit2.child)
      }
    } else if(plan1.isInstanceOf[LocalLimit] || plan2.isInstanceOf[LocalLimit]) {//放开限制，跳过LocalLimit
      if(plan1.isInstanceOf[LocalLimit] && plan2.isInstanceOf[LocalLimit]) {
        val localLimit1 = plan1.asInstanceOf[LocalLimit]
        val localLimit2 = plan2.asInstanceOf[LocalLimit]
        mergeTwoPlans(localLimit1.child, localLimit2.child)
      } else if(plan1.isInstanceOf[LocalLimit]) {
        val localLimit1 = plan1.asInstanceOf[LocalLimit]
        mergeTwoPlans(localLimit1.child, plan2)
      } else {
        val localLimit2 = plan2.asInstanceOf[LocalLimit]
        mergeTwoPlans(plan1, localLimit2.child)
      }
    } else if(plan1.isInstanceOf[Sort] || plan2.isInstanceOf[Sort]) {//对于排序算子，不进行合并
      if(plan1.isInstanceOf[Sort] && plan2.isInstanceOf[Sort]) {
        val sort1 = plan1.asInstanceOf[Sort]
        val sort2 = plan2.asInstanceOf[Sort]
        mergeTwoPlans(sort1.child, sort2.child)
      } else if(plan1.isInstanceOf[Sort]) {
        val sort1 = plan1.asInstanceOf[Sort]
        mergeTwoPlans(sort1.child, plan2)
      } else {
        val sort2 = plan2.asInstanceOf[Sort]
        mergeTwoPlans(plan1, sort2.child)
      }
    } else  {
      plan1
    }
  }
}
