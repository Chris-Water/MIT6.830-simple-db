package simpledb.optimizer;

import simpledb.ParsingException;
import simpledb.common.Database;
import simpledb.execution.*;

import javax.swing.*;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The JoinOptimizer class is responsible for ordering a series of joins
 * optimally, and for selecting the best instantiation of a join for a given
 * logical plan.
 */
public class JoinOptimizer {
    final LogicalPlan p;
    final List<LogicalJoinNode> joins;

    /**
     * Constructor
     *
     * @param p     the logical plan being optimized
     * @param joins the list of joins being performed
     */
    public JoinOptimizer(LogicalPlan p, List<LogicalJoinNode> joins) {
        this.p = p;
        this.joins = joins;
    }

    /**
     * Return best itera0.tor for computing a given logical join, given the
     * specified statistics, and the provided left and right subplans. Note that
     * there is insufficient information to determine which plan should be the
     * inner/outer here -- because OpIterator's don't provide any cardinality
     * estimates, and stats only has information about the base tables. For this
     * reason, the plan1 be the inner
     *
     * @param lj    The join being considered
     * @param plan1 The left join node's child
     * @param plan2 The right join node's child
     */
    public static OpIterator instantiateJoin(LogicalJoinNode lj, OpIterator plan1, OpIterator plan2) throws ParsingException {

        int t1id = 0, t2id = 0;
        OpIterator j;

        try {
            t1id = plan1.getTupleDesc().fieldNameToIndex(lj.f1QuantifiedName);
        } catch (NoSuchElementException e) {
            throw new ParsingException("Unknown field " + lj.f1QuantifiedName);
        }

        if (lj instanceof LogicalSubplanJoinNode) {
            t2id = 0;
        } else {
            try {
                t2id = plan2.getTupleDesc().fieldNameToIndex(lj.f2QuantifiedName);
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown field " + lj.f2QuantifiedName);
            }
        }

        JoinPredicate p = new JoinPredicate(t1id, lj.p, t2id);

        if (lj.p == Predicate.Op.EQUALS) {

            try {
                // dynamically load HashEquiJoin -- if it doesn't exist, just
                // fall back on regular join
                Class<?> c = Class.forName("simpledb.execution.HashEquiJoin");
                java.lang.reflect.Constructor<?> ct = c.getConstructors()[0];
                j = (OpIterator) ct.newInstance(new Object[]{p, plan1, plan2});
            } catch (Exception e) {
                j = new Join(p, plan1, plan2);
            }
        } else {
            j = new Join(p, plan1, plan2);
        }

        return j;

    }

    /**
     * Estimate the join cardinality of two tables.
     * <p>
     * 1.For equality joins, when one of the attributes is a primary key,
     * the number of tuples produced by the join
     * cannot be larger than the cardinality of the non-primary key attribute.
     * <p>
     * 2.For equality joins when there is no primary key,
     * it's hard to say much about what the size of the output is,
     * it could be the size of the product of the cardinalities of the tables
     * (if both tables have the same value for all tuples)
     * or it could be 0. It's fine to make up a simple heuristic
     * (say, the size of the larger of the two tables).
     * <p>
     * 3.For range scans, it is similarly hard to say anything accurate about sizes.
     * The size of the output should be proportional to the sizes of the inputs.
     * It is fine to assume that a fixed fraction of the cross-product is emitted by range scans (say, 30%).
     * In general, the cost of a range join should be larger than the cost of a non-primary key equality join of two tables of the same size.
     */
    public static int estimateTableJoinCardinality(Predicate.Op joinOp, String table1Alias, String table2Alias, String field1PureName, String field2PureName, int card1, int card2, boolean t1pkey, boolean t2pkey, Map<String, TableStats> stats, Map<String, Integer> tableAliasToId) {
        int card = 1;
        // some code goes here
        /**
         * 估计联接基数的另一种方法是假设较小表中的每个值在较大表中都有匹配值。
         * 那么连接选择性的公式是: 1/(Max (num-distinct (t1，column1) ，num-distinct (t2，column2))))。
         * 这里，column1和 column2是联接属性。连接的基数是 t1和 t2的基数乘以选择性的产物。
         */
        if (joinOp == Predicate.Op.EQUALS) {
            //case2: no primary key
            if (!t1pkey && !t2pkey) {
                card = Math.max(card1, card2);
            } else if (t1pkey && t2pkey) {
                //case1: one of the attributes is a primary key
                card = Math.min(card1, card2);
            } else {
                //case1: one of the attributes is a primary key
                card = t1pkey ? card2 : card1;
            }
        } else {
            card = (int) (0.3 * card1 * card2);
        }
        return card <= 0 ? 1 : card;
    }

    public static void main(String[] args) {
        JoinOptimizer optimizer = new JoinOptimizer(null, null);
        List<Integer> list = IntStream.range(0, 5).boxed().collect(Collectors.toList());
        long s = System.currentTimeMillis();
        Set<Set<Integer>> sets = optimizer.enumerateSubsetsOptimal(list, 3);
        long e = System.currentTimeMillis();
        System.out.println("count = " + sets.size() + ",cost = " + (e - s) + "ms");

    }

    /**
     * Estimate the cost of a join.
     * <p>
     * The cost of the join should be calculated based on the join algorithm (or
     * algorithms) that you implemented for Lab 2. It should be a function of
     * the amount of data that must be read over the course of the query, as
     * well as the number of CPU opertions performed by your join. Assume that
     * the cost of a single predicate application is roughly 1.
     *
     * @param j     A LogicalJoinNode representing the join operation being
     *              performed.
     * @param card1 Estimated cardinality of the left-hand side of the query
     * @param card2 Estimated cardinality of the right-hand side of the query
     * @param cost1 Estimated cost of one full scan of the table on the left-hand
     *              side of the query
     * @param cost2 Estimated cost of one full scan of the table on the right-hand
     *              side of the query
     * @return An estimate of the cost of this query, in terms of cost1 and
     * cost2
     */
    public double estimateJoinCost(LogicalJoinNode j, int card1, int card2, double cost1, double cost2) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 3.
            return card1 + cost1 + cost2;
        } else {
            // Insert your code here.
            // HINT: You may need to use the variable "j" if you implemented
            // a join algorithm that's more complicated than a basic
            // nested-loops join.
            // joincost(t1 join t2) = scancost(t1) + ntups(t1) x scancost(t2) //IO cost
            //                       + ntups(t1) x ntups(t2)  //CPU cost
            return cost1 + card1 * cost2 + card1 * card2;
        }
    }

    /**
     * Estimate the cardinality of a join. The cardinality of a join is the
     * number of tuples produced by the join.
     *
     * @param j      A LogicalJoinNode representing the join operation being
     *               performed.
     * @param card1  Cardinality of the left-hand table in the join
     * @param card2  Cardinality of the right-hand table in the join
     * @param t1pkey Is the left-hand table a primary-key table?
     * @param t2pkey Is the right-hand table a primary-key table?
     * @param stats  The table stats, referenced by table names, not alias
     * @return The cardinality of the join
     */
    public int estimateJoinCardinality(LogicalJoinNode j, int card1, int card2, boolean t1pkey, boolean t2pkey, Map<String, TableStats> stats) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 3.
            return card1;
        } else {
            return estimateTableJoinCardinality(j.p, j.t1Alias, j.t2Alias, j.f1PureName, j.f2PureName, card1, card2, t1pkey, t2pkey, stats, p.getTableAliasToIdMapping());
        }
    }

    /**
     * Helper method to enumerate all of the subsets of a given size of a
     * specified vector.
     *
     * @param v    The vector whose subsets are desired
     * @param size The size of the subsets of interest
     * @return a set of all subsets of the specified size
     */
    public <T> Set<Set<T>> enumerateSubsets(List<T> v, int size) {
        Set<Set<T>> els = new HashSet<>();
        els.add(new HashSet<>());
        for (int i = 0; i < size; i++) {
            Set<Set<T>> newels = new HashSet<>();
            for (Set<T> s : els) {
                for (T t : v) {
                    if (s.contains(t)) {
                        continue;
                    }
                    Set<T> news = new HashSet<>(s);
                    news.add(t);
                    newels.add(news);
                }
            }
            els = newels;
        }
        return els;

    }

    /**
     * 例如 v = [0,1,2,3,4]  size = 3
     * 用二进制数0 ~ 2^(v.length) - 1表示每个下标的元素是否选取，对应位取1表示选择对应下标的元素
     * 如 [0,1,0,1,1] 从低位到高位表示 子集 [1,3,4] 对应的数字为26
     * <p>
     * 在这个优化的算法中，我们使用一个循环来遍历从 0 到 2^n-1 的所有数字。
     * 这些数字的二进制表示形式刚好可以表示子集中元素的选择情况。
     * 在循环中，我们使用一个内部循环来提取每个数字 i 的二进制位。
     * 我们通过将 i 右移并检查最低位来提取二进制位的值。如果最低位为 1，则将对应位置上的元素添加到子集中。
     * <p>
     * 与位运算的方法相比，这种迭代法的优势在于它不需要进行位运算和计算二进制位数，
     * 而是通过递增地处理数字 i 和列表索引来直接生成子集。这使得算法更加高效且易于理解。
     * 这种迭代法的时间复杂度仍然是 O(2^n * n)，但它的效率通常比位运算的方法稍高。
     *
     * @param v
     * @param size
     * @param <T>
     * @return
     */
    public <T> Set<Set<T>> enumerateSubsetsOptimal(List<T> v, int size) {
        Set<Set<T>> subsets = new HashSet<>();
        int n = v.size();
        int max = 1 << n; // 2^n
        for (int i = 0; i < max; i++) {
            if (Integer.bitCount(i) == size) {
                //当子集中元素满足size要求时收集结果
                Set<T> subset = new HashSet<>();
                int index = 0;
                int j = i;
                //通过将 i 右移并检查最低位来提取二进制位的值。如果最低位为 1，则将对应位置上的元素添加到子集中。
                while (j > 0) {
                    if ((j & 1) == 1) {
                        subset.add(v.get(index));
                    }
                    j >>= 1;
                    index++;
                }
                subsets.add(subset);
            }
        }
        return subsets;
    }

    public <T> void genSubset(List<T> v, int size, Set<T> current, Set<Set<T>> result) {
        if (current.size() == size) {
            result.add(new HashSet<>(current));
            return;
        }
        for (T t : v) {
            if (!current.contains(t)) {
                current.add(t);
                genSubset(v, size, current, result);
                current.remove(t);
            }
        }
    }

    /**
     * Compute a logical, reasonably efficient join on the specified tables. See
     * PS4 for hints on how this should be implemented.
     *
     * @param stats               Statistics for each table involved in the join, referenced by
     *                            base table names, not alias
     * @param filterSelectivities Selectivities of the filter predicates on each table in the
     *                            join, referenced by table alias (if no alias, the base table
     *                            name)
     * @param explain             Indicates whether your code should explain its query plan or
     *                            simply execute it
     * @return A List<LogicalJoinNode> that stores joins in the left-deep
     * order in which they should be executed.
     * @throws ParsingException when stats or filter selectivities is missing a table in the
     *                          join, or or when another internal error occurs
     */
    public List<LogicalJoinNode> orderJoins(Map<String, TableStats> stats, Map<String, Double> filterSelectivities, boolean explain) throws ParsingException {
        // some code goes here
        //计算最优的Join
        PlanCache planCache = new PlanCache();
        for (int i = 1; i <= joins.size(); i++) {
            //枚举长度为i的子集
            Set<Set<LogicalJoinNode>> subsets = enumerateSubsetsOptimal(joins, i);
            //计算最优子集的costCard
            for (Set<LogicalJoinNode> subset : subsets) {
                CostCard bestPlan = new CostCard();
                bestPlan.cost = Double.MAX_VALUE;
                for (LogicalJoinNode joinToRemove : subset) {
                    // 计算 (subset-joinToRemove) join joinToRemove 的cost,card,order
                    CostCard planCost = computeCostAndCardOfSubplan(stats, filterSelectivities, joinToRemove, subset, bestPlan.cost, planCache);
                    if (planCost == null) {
                        continue;
                    }
                    if (planCost.cost < bestPlan.cost) {
                        //cost更小则更新计划
                        bestPlan = planCost;
                    }
                }
                planCache.addPlan(subset, bestPlan.cost, bestPlan.card, bestPlan.plan);
            }
        }
        return planCache.getOrder(new HashSet<>(joins));
    }

    // ===================== Private Methods =================================

    /**
     * This is a helper method that computes the cost and cardinality of joining
     * joinToRemove to joinSet (joinSet should contain joinToRemove), given that
     * all of the subsets of size joinSet.size() - 1 have already been computed
     * and stored in PlanCache pc.
     *
     * @param stats               table stats for all of the tables, referenced by table names
     *                            rather than alias (see {@link #orderJoins})
     * @param filterSelectivities the selectivities of the filters over each of the tables
     *                            (where tables are indentified by their alias or name if no
     *                            alias is given)
     * @param joinToRemove        the join to remove from joinSet
     * @param joinSet             the set of joins being considered
     * @param bestCostSoFar       the best way to join joinSet so far (minimum of previous
     *                            invocations of computeCostAndCardOfSubplan for this joinSet,
     *                            from returned CostCard)
     * @param pc                  the PlanCache for this join; should have subplans for all
     *                            plans of size joinSet.size()-1
     * @return A {@link CostCard} objects desribing the cost, cardinality,
     * optimal subplan
     * @throws ParsingException when stats, filterSelectivities, or pc object is missing
     *                          tables involved in join
     */
    @SuppressWarnings("unchecked")
    private CostCard computeCostAndCardOfSubplan(Map<String, TableStats> stats, Map<String, Double> filterSelectivities, LogicalJoinNode joinToRemove, Set<LogicalJoinNode> joinSet, double bestCostSoFar, PlanCache pc) throws ParsingException {

        LogicalJoinNode j = joinToRemove;

        List<LogicalJoinNode> prevBest;

        if (this.p.getTableId(j.t1Alias) == null) {
            throw new ParsingException("Unknown table " + j.t1Alias);
        }
        if (this.p.getTableId(j.t2Alias) == null) {
            throw new ParsingException("Unknown table " + j.t2Alias);
        }

        String table1Name = Database.getCatalog().getTableName(this.p.getTableId(j.t1Alias));
        String table2Name = Database.getCatalog().getTableName(this.p.getTableId(j.t2Alias));
        String table1Alias = j.t1Alias;
        String table2Alias = j.t2Alias;

        Set<LogicalJoinNode> news = new HashSet<>(joinSet);
        news.remove(j);

        double t1cost, t2cost;
        int t1card, t2card;
        boolean leftPkey, rightPkey;

        if (news.isEmpty()) { // base case -- both are base relations
            prevBest = new ArrayList<>();
            t1cost = stats.get(table1Name).estimateScanCost();
            t1card = stats.get(table1Name).estimateTableCardinality(filterSelectivities.get(j.t1Alias));
            leftPkey = isPkey(j.t1Alias, j.f1PureName);

            t2cost = table2Alias == null ? 0 : stats.get(table2Name).estimateScanCost();
            t2card = table2Alias == null ? 0 : stats.get(table2Name).estimateTableCardinality(filterSelectivities.get(j.t2Alias));
            rightPkey = table2Alias != null && isPkey(table2Alias, j.f2PureName);
        } else {
            // news is not empty -- figure best way to join j to news
            prevBest = pc.getOrder(news);

            // possible that we have not cached an answer, if subset
            // includes a cross product
            if (prevBest == null) {
                return null;
            }

            double prevBestCost = pc.getCost(news);
            int bestCard = pc.getCard(news);

            // estimate cost of right subtree
            if (doesJoin(prevBest, table1Alias)) { // j.t1 is in prevBest
                t1cost = prevBestCost; // left side just has cost of whatever
                // left
                // subtree is
                t1card = bestCard;
                leftPkey = hasPkey(prevBest);

                t2cost = j.t2Alias == null ? 0 : stats.get(table2Name).estimateScanCost();
                t2card = j.t2Alias == null ? 0 : stats.get(table2Name).estimateTableCardinality(filterSelectivities.get(j.t2Alias));
                rightPkey = j.t2Alias != null && isPkey(j.t2Alias, j.f2PureName);
            } else if (doesJoin(prevBest, j.t2Alias)) { // j.t2 is in prevbest
                // (both
                // shouldn't be)
                t2cost = prevBestCost; // left side just has cost of whatever
                // left
                // subtree is
                t2card = bestCard;
                rightPkey = hasPkey(prevBest);
                t1cost = stats.get(table1Name).estimateScanCost();
                t1card = stats.get(table1Name).estimateTableCardinality(filterSelectivities.get(j.t1Alias));
                leftPkey = isPkey(j.t1Alias, j.f1PureName);

            } else {
                // don't consider this plan if one of j.t1 or j.t2
                // isn't a table joined in prevBest (cross product)
                return null;
            }
        }

        // case where prevbest is left
        double cost1 = estimateJoinCost(j, t1card, t2card, t1cost, t2cost);

        LogicalJoinNode j2 = j.swapInnerOuter();
        double cost2 = estimateJoinCost(j2, t2card, t1card, t2cost, t1cost);
        if (cost2 < cost1) {
            boolean tmp;
            j = j2;
            cost1 = cost2;
            tmp = rightPkey;
            rightPkey = leftPkey;
            leftPkey = tmp;
        }
        if (cost1 >= bestCostSoFar) {
            return null;
        }

        CostCard cc = new CostCard();

        cc.card = estimateJoinCardinality(j, t1card, t2card, leftPkey, rightPkey, stats);
        cc.cost = cost1;
        cc.plan = new ArrayList<>(prevBest);
        cc.plan.add(j); // prevbest is left -- add new join to end
        return cc;
    }

    /**
     * Return true if the specified table is in the list of joins, false
     * otherwise
     */
    private boolean doesJoin(List<LogicalJoinNode> joinlist, String table) {
        for (LogicalJoinNode j : joinlist) {
            if (j.t1Alias.equals(table) || (j.t2Alias != null && j.t2Alias.equals(table))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return true if field is a primary key of the specified table, false
     * otherwise
     *
     * @param tableAlias The alias of the table in the query
     * @param field      The pure name of the field
     */
    private boolean isPkey(String tableAlias, String field) {
        int tid1 = p.getTableId(tableAlias);
        String pkey1 = Database.getCatalog().getPrimaryKey(tid1);

        return pkey1.equals(field);
    }

    /**
     * Return true if a primary key field is joined by one of the joins in
     * joinlist
     */
    private boolean hasPkey(List<LogicalJoinNode> joinlist) {
        for (LogicalJoinNode j : joinlist) {
            if (isPkey(j.t1Alias, j.f1PureName) || (j.t2Alias != null && isPkey(j.t2Alias, j.f2PureName))) {
                return true;
            }
        }
        return false;

    }

    /**
     * Helper function to display a Swing window with a tree representation of
     * the specified list of joins. See {@link #orderJoins}, which may want to
     * call this when the analyze flag is true.
     *
     * @param js            the join plan to visualize
     * @param pc            the PlanCache accumulated whild building the optimal plan
     * @param stats         table statistics for base tables
     * @param selectivities the selectivities of the filters over each of the tables
     *                      (where tables are indentified by their alias or name if no
     *                      alias is given)
     */
    private void printJoins(List<LogicalJoinNode> js, PlanCache pc, Map<String, TableStats> stats, Map<String, Double> selectivities) {

        JFrame f = new JFrame("Join Plan for " + p.getQuery());

        // Set the default close operation for the window,
        // or else the program won't exit when clicking close button
        f.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

        f.setVisible(true);

        f.setSize(300, 500);

        Map<String, DefaultMutableTreeNode> m = new HashMap<>();

        // int numTabs = 0;

        // int k;
        DefaultMutableTreeNode root = null, treetop = null;
        HashSet<LogicalJoinNode> pathSoFar = new HashSet<>();
        boolean neither;

        System.out.println(js);
        for (LogicalJoinNode j : js) {
            pathSoFar.add(j);
            System.out.println("PATH SO FAR = " + pathSoFar);

            String table1Name = Database.getCatalog().getTableName(this.p.getTableId(j.t1Alias));
            String table2Name = Database.getCatalog().getTableName(this.p.getTableId(j.t2Alias));

            // Double c = pc.getCost(pathSoFar);
            neither = true;

            root = new DefaultMutableTreeNode("Join " + j + " (Cost =" + pc.getCost(pathSoFar) + ", card = " + pc.getCard(pathSoFar) + ")");
            DefaultMutableTreeNode n = m.get(j.t1Alias);
            if (n == null) { // never seen this table before
                n = new DefaultMutableTreeNode(j.t1Alias + " (Cost = " + stats.get(table1Name).estimateScanCost() + ", card = " + stats.get(table1Name).estimateTableCardinality(selectivities.get(j.t1Alias)) + ")");
                root.add(n);
            } else {
                // make left child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t1Alias, root);

            n = m.get(j.t2Alias);
            if (n == null) { // never seen this table before

                n = new DefaultMutableTreeNode(j.t2Alias == null ? "Subplan" : (j.t2Alias + " (Cost = " + stats.get(table2Name).estimateScanCost() + ", card = " + stats.get(table2Name).estimateTableCardinality(selectivities.get(j.t2Alias)) + ")"));
                root.add(n);
            } else {
                // make right child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t2Alias, root);

            // unless this table doesn't join with other tables,
            // all tables are accessed from root
            if (!neither) {
                for (String key : m.keySet()) {
                    m.put(key, root);
                }
            }

            treetop = root;
        }

        JTree tree = new JTree(treetop);
        JScrollPane treeView = new JScrollPane(tree);

        tree.setShowsRootHandles(true);

        // Set the icon for leaf nodes.
        ImageIcon leafIcon = new ImageIcon("join.jpg");
        DefaultTreeCellRenderer renderer = new DefaultTreeCellRenderer();
        renderer.setOpenIcon(leafIcon);
        renderer.setClosedIcon(leafIcon);

        tree.setCellRenderer(renderer);

        f.setSize(300, 500);

        f.add(treeView);
        for (int i = 0; i < tree.getRowCount(); i++) {
            tree.expandRow(i);
        }

        if (js.size() == 0) {
            f.add(new JLabel("No joins in plan."));
        }

        f.pack();

    }

}
