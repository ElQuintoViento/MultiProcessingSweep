# -*- coding: utf-8 -*-
from multiprocessing import JoinableQueue, Process, Queue, cpu_count
import sys
from time import time


class SweepResult(object):
    def __init__(self, error, params):
        self.error = error
        self.params = params

    def __str__(self):
        return ("Error: {} \t Params: {}".format(self.error, self.params))


class SweepTask(object):
    def __init__(self, function, params_list, tuples_list, top_n,
                 order_every_n):
        if not hasattr(function, '__call__'):
            raise Exception("No function was provided")
        if not isinstance(params_list, (list,)):
            raise Exception("No parameter list provided")
        if not isinstance(tuples_list, (list,)):
            raise Exception("No tuples list provided")
        self.function = function
        self.params_list = params_list
        self.tuples_list = tuples_list
        self.top_n = top_n
        self.order_every_n = order_every_n

    def __call__(self):
        results = []
        count = 0
        for params in self.params_list:
            total = 0
            diff = 0
            for tple in self.tuples_list:
                r = self.function(params, tple)
                total += tple[len(tple) - 1]
                diff += abs(r - tple[len(tple) - 1])
            #
            results.append(SweepResult(float(diff) / float(total), params))
            #
            count += 1
            #
            if count >= max(self.top_n * 5, self.order_every_n):
                count = self.top_n
                results.sort(key=lambda x: x.error)
                results = results[:count]
        #
        results.sort(key=lambda x: x.error)
        if count > self.top_n:
            results = results[:self.top_n]
        #
        return results


class SweepConsumer(Process):
    def __init__(self, task_queue, result_queue, **kwargs):
        Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.timeout = 10
        if 'timeout' in kwargs:
            self.timeout = float(kwargs['timeout'])

    def run(self):
        counter = -1
        try_again = True

        while True:
            counter += 1
            try:
                task = self.task_queue.get(True, timeout=self.timeout)
                # print("{} finished task {}".format(self.name, counter))
                try_again = True
            except Exception as e:
                sys.stdout.write(str(e))
                if try_again:
                    try_again = False
                    continue
                else:
                    return
            #
            results = task()
            self.task_queue.task_done()
            self.result_queue.put(results)
            # print("\t\tadded results")
        print("{} FINISHED".format(self.name))


def sort_and_reduce(lst, size, sort_by_attr):
    lst.sort(key=lambda x: x.__dict__[sort_by_attr])
    if len(lst) > size:
        del lst[size:]


def run_sweep(function, params_lists_list, tuples_list, **kwargs):
    # Only keep best n
    top_n = 10
    if 'top_n' in kwargs:
        top_n = int(kwargs['top_n'])
    # Group parameter lists by this amount
    group_count = top_n * 10
    if 'group_count' in kwargs:
        group_count = int(kwargs['group_count'])
    # Order after this amount
    order_every_n = int(group_count / 3)
    if 'order_every_n' in kwargs:
        order_every_n = int(kwargs['order_every_n'])
    #
    task_queue = JoinableQueue()
    result_queue = Queue()
    number_of_consumers = int(2.0 * float(cpu_count()))
    #
    print("Started {} consumers using {} CPUs".format(
        number_of_consumers, cpu_count()))
    # Setup initial parameter grouping quantities
    count = 0
    task_count = 0
    divisors = []
    size = 1
    for i in range(len(params_lists_list) - 1, -1, -1):
        size *= len(params_lists_list[i])
        divisors.insert(0, len(params_lists_list[i]))
        if i < (len(params_lists_list) - 1):
            divisors[0] *= divisors[1]
    # shift divisors
    for i in range(0, len(params_lists_list) - 1):
        divisors[i] = divisors[i + 1]
    divisors[len(divisors) - 1] = 1
    # Start consumers
    consumers = [SweepConsumer(task_queue, result_queue)
                 for i in range(0, number_of_consumers)]
    #
    for c in consumers:
        c.start()
    #
    print("Added 0 of {} param combinations".format(size))
    params_list = []
    results = []
    for i in xrange(0, size):
        params = []
        for j in range(0, len(params_lists_list)):
            index = (i / divisors[j]) % len(params_lists_list[j])
            params.append(params_lists_list[j][index])
        # print("\t{} \t {}".format(i, str(params)))
        params_list.append(params)
        count += 1
        #
        if count >= group_count:
            # print(" - - - - - - - - - -\n{}\n".format(params_list))
            task_queue.put(
                SweepTask(
                    function, params_list, tuples_list, top_n, order_every_n))
            task_count += 1
            params_list = []
            count = 0
            # Wait ?
            # while task_queue.qsize() > (3 * len(consumers)):
            #    continue
            #
            sys.stdout.write("\033[F")
            print("Added {} of {} param combinations".format(i, size))
    #
    if count > 0:
        # print(" - - - - - - - - - -\n{}\n".format(params_list))
        task_queue.put(
            SweepTask(
                function, params_list, tuples_list, top_n, order_every_n))
        task_count += 1
    # Start sorting our results
    print("Getting results 0%")
    for i in range(0, task_count):
        sys.stdout.write("\033[F")
        print("Getting results {:.2f}%".format(float(i) / float(task_count)))
        result = result_queue.get(True)
        results += result
        #
        if len(results) > order_every_n:
            sort_and_reduce(results, top_n, 'error')
    #
    sort_and_reduce(results, top_n, 'error')
    # print("here 2")
    # Make sure tasks are finished
    task_queue.join()
    #
    for result in results:
        print("{}".format(result))


def dumby_function(params, tple):
    return (params[0] * tple[0] + params[1] * tple[1])


# run_sweep("derp", p_ls_l, "")
if __name__ == "__main__":
    params_lists_list = [
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
         20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36,
         37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53,
         54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
         71, 72, 73, 74, 75, 76, 77, 78, 79],
        [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
         20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36,
         37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53,
         54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
         71, 72, 73, 74, 75, 76, 77, 78, 79]
    ]
    #
    tuples_list = [
        (0, 1, 11), (1, 3, 41), (2, 5, 71), (3, 7, 101), (4, 9, 131),
        (5, 11, 161), (6, 13, 191), (7, 15, 221), (8, 17, 251), (9, 19, 281)
    ]
    run_sweep(dumby_function, params_lists_list, tuples_list)
