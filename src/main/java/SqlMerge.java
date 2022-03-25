import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class SqlMerge {
    public static String filePath = "C:\\Users\\17799\\Desktop\\materized view\\data\\similarSql";
    public static String appName ="root";
    public static SparkSession sparkSession = null;
    public static String warehouseLocation = "C:\\tmp\\hive";

    public void initSparkSession() {
        if(sparkSession == null) {
             sparkSession = SparkSession.builder().master("local").appName(appName)
                     .config("spark.sql.warehouse.dir", warehouseLocation)
                     .config("spark.sql.execution.arrow.enabled", "true")
                     .config("spark.sql.catalogImplementation","hive")
                     .config("hive.metastore.warehouse.dir",warehouseLocation)
                     .config("hive.exec.scratchdir", warehouseLocation)
                     .getOrCreate();
        }
    }

    public SparkSession getSparkSession() {
        if(sparkSession == null) {
            initSparkSession();
        }
        return sparkSession;
    }

    public void createTable() {
        SparkSession sparkSession = getSparkSession();
        sparkSession.sql("create table table2 (a int, b int, c int, d int)");
    }

    /**
     * 返回需要合并的logicalplan集合
     * @return
     */
    public List<LogicalPlan> getLogicalPlansFromFile(String filePath) throws IOException {
        FileInputStream inputStream = new FileInputStream(filePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        List<LogicalPlan> similarLogicalPlans = new ArrayList<LogicalPlan>();
        String str = null;
        SparkSession sparkSession = getSparkSession();
        while((str = bufferedReader.readLine()) != null)
        {
            LogicalPlan logicalPlan = sparkSession.sql(str).queryExecution().analyzed();
            similarLogicalPlans.add(logicalPlan);
        }
        //close
        inputStream.close();
        bufferedReader.close();
        return similarLogicalPlans;
    }

    /**
     * 将参数中的多个相似的logicalplan合并成一个logicplan
     * @param similarLogicalPlans
     * @return
     */
    public LogicalPlan mergeLogicPlan(List<LogicalPlan> similarLogicalPlans) {
        return null;
    }

    public static void main(String[] args) {
        SqlMerge sqlMerge = new SqlMerge();
        try {
            //sqlMerge.createTable();
            List<LogicalPlan> logicalPlanList = sqlMerge.getLogicalPlansFromFile(filePath);
            MergePlan mergePlan = new MergePlan();
            LogicalPlan logicalPlan = mergePlan.prePostProcess(logicalPlanList.get(0), logicalPlanList.get(1));
            System.out.println(logicalPlan.prettyJson());
            SQLBuilder sqlBuilder = new SQLBuilder(logicalPlan);
            System.out.println(sqlBuilder.toSQL());
            //sqlMerge.mergeLogicPlan(logicalPlanList);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
