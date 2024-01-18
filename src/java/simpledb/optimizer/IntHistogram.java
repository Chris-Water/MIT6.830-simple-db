package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;

    private int max;
    private int count;
    private Map<Range, Integer> gram = new LinkedHashMap<>();

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        count = 0;
        //初始化直方图
        int dis = (int) Math.ceil((double) (max - min) / buckets);
        for (int i = 0, begin = min; i < buckets; i++, begin += dis) {
            if (i != buckets - 1) {
                gram.put(new Range(begin, begin + dis - 1), 0);
            } else {
                gram.put(new Range(begin, max), 0);
            }
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if (v > max || v < min) {
            //超出范围的值不加入
            return;
        }
        // some code goes here
        Optional<Range> bucket = gram.keySet().stream().filter(r -> r.inRange(v) == 0).findFirst();
        bucket.ifPresent(range -> gram.put(range, gram.get(range) + 1));
        count++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     * 时间复杂度为 bucket的个数
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if (v > max || v < min) {
            //超出范围的值直接返回
            double res = 0;
            switch (op) {
                case EQUALS:
                    break;
                case NOT_EQUALS:
                    res = 1;
                    break;
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                    res = v < min ? 1.0 : 0;
                    break;
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                    res = v > max ? 1.0 : 0;
                    break;

                default:
            }
            return res;
        }
        //等于
        double eq = 0;
        //大于
        double gt = 0;
        //小于
        double lt = 0;
        //桶内部分小于
        double bLess = 0;
        //桶内部分大于
        double bGreater = 0;
        for (Range range : gram.keySet()) {
            int inRange = range.inRange(v);
            Integer h = gram.get(range);
            double bF = (double) h / count;
            if (inRange == 0) {
                //计算相等值 (1/ntups)* h/w
                eq = bF / range.width();
                bLess = bF * (v - range.begin) / range.width();
                bGreater = bF * (range.end - v) / range.width();
            } else if (inRange == 1) {
                //计算小于的部分 h/ntups
                lt += bF;
            } else {
                //计算大于的部分
                gt += bF;
            }
        }
        double res = 0;
        switch (op) {
            case EQUALS:
                res += eq;
                break;
            case GREATER_THAN:
                res += gt + bGreater;
                break;
            case GREATER_THAN_OR_EQ:
                res += gt + bGreater + eq;
                break;
            case LESS_THAN:
                res += lt + bLess;
                break;
            case LESS_THAN_OR_EQ:
                res += lt + bLess + eq;
                break;
            case NOT_EQUALS:
                res = 1 - eq;
                break;
            default:
        }
        return res;
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        // some code goes here
        return gram.toString();
    }

    private static class Range {
        final int begin;
        final int end;

        public Range(int begin, int end) {
            this.begin = begin;
            this.end = end;
        }

        @Override
        public String toString() {
            return "Range[" + begin + "," + end + ']';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Range range = (Range) o;
            return begin == range.begin && end == range.end;
        }

        @Override
        public int hashCode() {
            int result = begin;
            result = 31 * result + end;
            return result;
        }

        public int inRange(int v) {
            if (v >= begin && v <= end) {
                return 0;
            } else if (v < begin) {
                return -1;
            } else {
                return 1;
            }
        }

        public int width() {
            return end - begin + 1;
        }
    }
}
