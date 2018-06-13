package edu.coursera.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */
public final class ReciprocalArraySum {

    /**
     * Default constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Sequentially compute the sum of the reciprocal values for a given array.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        // Compute sum of reciprocals of array elements
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }

        return sum;
    }

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     *
     * @param nChunks The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Computes the inclusive element index that the provided chunk starts at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the start of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The inclusive index that this chunk starts at in the set of
     *         nElements
     */
    private static int getChunkStartInclusive(final int chunk,
            final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Computes the exclusive element index that the provided chunk ends at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the end of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The exclusive end index for this chunk
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {

        /**
         * Starting index for traversal done by this task.
         */
        private final int startIndexInclusive;
        /**
         * Ending index for traversal done by this task.
         */
        private final int endIndexExclusive;
        /**
         * Input array to reciprocal sum.
         */
        private final double[] input;
        /**
         * Intermediate value produced by this task.
         */
        private double value;

        private int threshhold;

        /**
         * Constructor.
         * @param setStartIndexInclusive Set the starting index to begin
         *        parallel traversal at.
         * @param setEndIndexExclusive Set ending index for parallel traversal.
         * @param setInput Input values
         */
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                final int setEndIndexExclusive, final double[] setInput, final int threshhold) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
            this.threshhold = threshhold;
        }

        /**
         * Getter for the value produced by this task.
         * @return Value produced by this task
         */
        public double getValue() {
            return value;
        }

        @Override
        protected void compute() {
             //int threshhold = 500001;

            //threshhold = 125001;

            //if(this.input.length > PAR_THRESHHOLD){
            if(endIndexExclusive - startIndexInclusive > threshhold){
                //do work in parallel

                System.out.println("Splitting, threshhold: " + threshhold);

                int startLeft = startIndexInclusive;
                int endLeft = ((endIndexExclusive-startIndexInclusive)/2)+startIndexInclusive;

                int startRight = ((endIndexExclusive-startIndexInclusive)/2)+startIndexInclusive+1;
                int endRight = endIndexExclusive;

                System.out.println(String.format("sLeft %s, eLeft %s, sRight %s, eRight %s", startLeft, endLeft, startRight, endRight));


                ReciprocalArraySumTask left = new ReciprocalArraySumTask(startLeft, endLeft, input, threshhold);
                ReciprocalArraySumTask right = new ReciprocalArraySumTask(startRight, endRight, input, threshhold);
                left.fork();
                right.compute();
                left.join();
                value = left.getValue() + right.getValue();
            }else {
                //do work sequential
                System.out.println("Working");
                for (int i = startIndexInclusive; i <= endIndexExclusive; i++) {
                    value += 1 / input[i];
                }
            }
        }
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumChunkedTask extends RecursiveAction {

        /**
         * Starting index for traversal done by this task.
         */
        private final int startIndexInclusive;
        /**
         * Ending index for traversal done by this task.
         */
        private final int endIndexExclusive;
        /**
         * Input array to reciprocal sum.
         */
        private final double[] input;
        /**
         * Intermediate value produced by this task.
         */
        private double value;

        /**
         * Constructor.
         * @param setStartIndexInclusive Set the starting index to begin
         *        parallel traversal at.
         * @param setEndIndexExclusive Set ending index for parallel traversal.
         * @param setInput Input values
         */
        ReciprocalArraySumChunkedTask(final int setStartIndexInclusive,
                               final int setEndIndexExclusive, final double[] setInput) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
        }

        /**
         * Getter for the value produced by this task.
         * @return Value produced by this task
         */
        public double getValue() {
            return value;
        }

        @Override
        protected void compute() {
            System.out.println(String.format("startIndexInclusive %s, endIndexExclusive %s", startIndexInclusive, endIndexExclusive));

            //do work sequential
                System.out.println("Working");
                for (int i = startIndexInclusive; i <= endIndexExclusive; i++) {
                    value += 1 / input[i];

            }
        }
    }

    /**
     * TODO: Modify this method to compute the same reciprocal sum as
     * seqArraySum, but use two tasks running in parallel under the Java Fork
     * Join framework. You may assume that the length of the input array is
     * evenly divisible by 2.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double parArraySum(final double[] input) {
        assert input.length % 2 == 0;

        int threshhold = (input.length/2)+1;

        ReciprocalArraySumTask task = new ReciprocalArraySumTask(0, input.length-1, input, threshhold);
        task.compute();
        return task.getValue();
    }

    /**
     * TODO: Extend the work you did to implement parArraySum to use a set
     * number of tasks to compute the reciprocal array sum. You may find the
     * above utilities getChunkStartInclusive and getChunkEndExclusive helpful
     * in computing the range of element indices that belong to each chunk.
     *
     * @param input Input array
     * @param numTasks The number of tasks to create
     * @return The sum of the reciprocals of the array input
     */
    protected static double parManyTaskArraySum(final double[] input,  int numTasks) {
//        int threshhold = input.length/numTasks+1;
//
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "8");
//
        System.out.println("___________________________________________________________________________");
//        System.out.println("Num tasks: " + numTasks + " num proc: " + Runtime.getRuntime().availableProcessors() + "threshhold: " + threshhold);
//
//        ReciprocalArraySumTask task = new ReciprocalArraySumTask(0, input.length-1, input, threshhold);
//        task.compute();
//        return task.getValue();

        //numTasks = 2;


        ForkJoinPool forkJoinPool = new ForkJoinPool(numTasks);
        forkJoinPool.

        List<ReciprocalArraySumChunkedTask> tasks = new ArrayList<>();
        int numOfItemsPerTask = input.length/numTasks;
        int startItem = 0;
        int endItem = numOfItemsPerTask -1;

        ReciprocalArraySumChunkedTask firstTask = new ReciprocalArraySumChunkedTask(startItem, endItem, input);
        //firstTask.fork();
        tasks.add(firstTask);
        for(int i=1; i<numTasks; i++){
            startItem = startItem + numOfItemsPerTask;
            endItem = endItem + numOfItemsPerTask;
            ReciprocalArraySumChunkedTask task = new ReciprocalArraySumChunkedTask(startItem, endItem, input);
            tasks.add(task);
            //task.compute();
        }
        ForkJoinTask.invokeAll(tasks);



        //firstTask.join();
        //double value = firstTask.getValue();
        double value = 0;

        for(ReciprocalArraySumChunkedTask task: tasks){
            value = value + task.getValue();
        }
        return value;

    }



}
