#!/usr/bin/env python
# coding: utf-8
###############################################################################
#
#    Biosketch
#
#    Copyright (C) 2023  QIMR Berghofer Medical Research Institute
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
###############################################################################

import io
import os
import pickle
import sys

from abc import ABC, abstractmethod
from logging import Logger
from multiprocessing import Pipe, Process, Queue
from multiprocessing.connection import Connection, wait

from typing import Any, BinaryIO, Callable, Optional, Sequence, Union

import dnaio

from tblib import pickling_support
from xopen import xopen

FilePath = Union[str, bytes, os.PathLike]
FileOpener = Callable[[str, str], BinaryIO]

pickling_support.install()


class Pipeline:
    """
    Runs a series of single-argument functions on some input data and returns
    the result. Functions can be dynamically added to the tasks list using the
    task() decorator method.
    """
    def __init__(self):
        self.tasks = []

    def task(self):
        def wrapper(func):
            self.tasks.append(func)
            return func
        return wrapper

    def run(self, data: Any) -> Any:
        result = data
        for task in self.tasks:
            result = task(result)
        return result


class FastxReader(Process):
    """
    A reader process that sends chunks of FASTX data to one or more workers.

    This class extends the Process class from the multiprocessing module, and
    is designed to be used in a multiprocessing setting to read FASTX data from
    a file and send it to worker processes in parallel. The FastxReader class
    is responsible for reading complete chunks of FASTA or FASTQ records from
    the file, and dispatching these chunks to the input connections of worker
    processes.

    Args:
        filename (FilePath): The path to the FASTX file to be read.
        connections (Sequence[Connection]): A sequence of output connections to
            send the chunks to worker processes.
        work_queue (Queue): A queue to receive the index of a worker process to
            send the next chunk to.
        error_queue (Queue): A queue for sending any exceptions that are raised
            during processing.
        opener (Optional[FileOpener]): The callable that will be used to open
            the FASTX file.
        buffer_size (Optional[int]): The size of each chunk to read in bytes.

    Notes:
        The reader opens the file and repeatedly reads complete chunks of FASTA
        or FASTQ records. Each time it reads a chunk, it gets a worker index
        from the work queue so it can send the chunk to the worker's input
        connection. Once all chunks have been dispatched, each worker is sent
        a poison-pill to signal the end of processing. 

        If an exception is raised, it is pickled together with its traceback
        and sent to the shared error queue so it can be handled in the main
        process.
    """
    def __init__(
        self,
        filename: FilePath,
        connections: Sequence[Connection],
        work_queue: Queue,
        error_queue: Queue,
        opener: Optional[FileOpener] = xopen,
        buffer_size: Optional[int] = None,
    ):
        super().__init__(name=self.__class__.__name__)
        self.filename = filename
        self.connections = connections
        self.work_queue = work_queue
        self.error_queue = error_queue
        self.opener = opener
        self.buffer_size = buffer_size

    def run(self):
        try:
            sys.stdin = os.fdopen(0)
            with self.opener(self.filename, 'rb') as reader:
                if self.buffer_size:
                    chunks = dnaio.read_chunks(reader, self.buffer_size)
                else:
                    chunks = dnaio.read_chunks(reader)

                self.distribute_chunks(chunks)

            self.poison_workers()

        except Exception as exc:
            self.error_queue.put(pickle.dumps(exc))
            for connection in self.connections:
                try:
                    connection.send(None)
                except OSError:
                    pass

        except KeyboardInterrupt:
            pass

    def distribute_chunks(self, chunks):
        chunk_num = 0
        for chunk in chunks:
            worker_idx = self.work_queue.get()
            connection = self.connections[worker_idx]
            connection.send(chunk_num)
            connection.send_bytes(chunk)

            chunk_num += 1

    def poison_workers(self):
        while not all(conn.closed for conn in self.connections):
            worker_idx = self.work_queue.get()
            connection = self.connections[worker_idx]
            connection.send(None)
            connection.close()


class FastxWorker(Process):
    """
    A worker process for the parallel processing of FASTX data.

    This class extends the Process class from the multiprocessing module, and
    is designed to be used in a multiprocessing setting to process FASTX data
    in parallel. The FastxWorker class is responsible for processing chunks of
    sequence records from the input connection and returning the bytes-like
    output across the output connection.

    Parameters:
        worker_idx (int): The index of the worker process.
        pipeline (Pipeline): The pipeline to process the sequence records.
        input_conn (Connection): The input connection for receiving data.
        output_conn (Connection): The output connection for sending data.
        work_queue (Queue): A queue for placing the worker's index when it is
            ready to receive data.
        error_queue (Queue): A queue for sending any exceptions raised during
            processing.

    Notes:
        When the worker is ready to receive data from the reader, the worker
        places its index onto the work queue before waiting to receive a chunk
        of sequence records from the input connection.

        If an exception is raised, it is pickled together with its traceback
        and sent to the shared error queue so it can be handled in the main
        process.
    """
    def __init__(
        self,
        worker_index: int,
        pipeline: Pipeline,
        input_conn: Connection,
        output_conn: Connection,
        work_queue: Queue,
        error_queue: Queue,
    ):
        super().__init__()
        self.worker_index = worker_index
        self.pipeline = pipeline
        self.input_conn = input_conn
        self.output_conn = output_conn
        self.work_queue = work_queue
        self.error_queue = error_queue

    def run(self):
        try:
            self.do_work()

        except Exception as exc:
            self.error_queue.put(pickle.dumps(exc))

        except KeyboardInterrupt:
            pass

    def do_work(self):
        while True:
            self.work_queue.put(self.worker_index)
            chunk_num = self.input_conn.recv()

            if chunk_num is None:
                break

            chunk = self.input_conn.recv_bytes()
            infile = io.BytesIO(chunk)

            with dnaio.open(infile) as reader:
                bytes_result = self.pipeline.run(reader)

            num_records = reader.number_of_records

            self.output_conn.send((chunk_num, num_records))
            self.output_conn.send_bytes(bytes_result)


class OrderedChunkWriter(Process):
    """
    A writer process that receives chunks of processed data from one or more
    worker processes and writes them to a file in the correct order.

    This class extends the Process class from the multiprocessing module, and
    is designed to be used in a multiprocessing setting to receive (unordered)
    chunks of processed data from one or more workers processes. The received
    chunks are cached in a dictionary before being retrieved in the correct
    order and written to the output file.

    Parameters:
        filename (FilePath): The path to the output file.
        connections (Sequence[Connection]): A sequence of connections to
            receive data from.
        error_queue (Queue): A queue for sending any exceptions that are raised
            during processing.
        opener (Optional[FileOpener]): An optional file opener function used to
            open the output file.

    Notes:
        Once all chunks have been written to the output file, the writer places
        a sentinel value onto the error queue.

        If an exception is raised, it is pickled together with its traceback
        and sent to the shared error queue so it can be handled in the main
        process.
    """
    def __init__(
        self,
        filename: FilePath,
        connections: Sequence[Connection],
        error_queue: Queue,
        opener: Optional[FileOpener] = xopen,
    ):
        super().__init__(name=self.__class__.__name__)
        self.filename = filename
        self.connections = connections
        self.error_queue = error_queue
        self.opener = opener

    def run(self):
        try:
            with self.opener(self.filename, 'wb') as outfile:
                for chunk in self.iter_chunks():
                    outfile.write(chunk)

            self.error_queue.put(None)

        except Exception as exc:
            self.error_queue.put(pickle.dumps(exc))

        except KeyboardInterrupt:
            pass

    def iter_ready_connections(self):
        while self.connections:
            for conn in wait(self.connections):
                yield conn

    def iter_chunks(self):
        chunk_cache = dict()
        chunk_counter = 0

        for conn in self.iter_ready_connections():
            try:
                chunk_num, count = conn.recv()
            except EOFError:
                self.connections.remove(conn)
                continue

            chunk_cache[chunk_num] = conn.recv_bytes()

            self.error_queue.put((chunk_num, count))

            while chunk_counter in chunk_cache:
                yield chunk_cache.pop(chunk_counter)

                chunk_counter += 1


class PipelineRunner(ABC):
    """
    Abstract base class for running a pipeline.

    Subclasses should implement the `run` method to define how the pipeline
    should be run.

    Parameters:
        pipeline (Pipeline): The pipeline to be executed.
    """
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline

    @abstractmethod
    def run(self):
        pass


class BasePipelineRunner(PipelineRunner):
    """
    Base class for running a read processing pipeline.

    This class extends the `PipelineRunner` abstract base class and provides
    additional functionality for running a read processing pipeline.

    Parameters:
        pipeline (Pipeline): The pipeline to process the sequence records.
        input_file (FilePath): The path to the input FASTX file.
        output_file (FilePath): The path to the output file.
        logger (Logger): The logger object for logging progress and messages.
        input_opener (Optional[FileOpener]): A callable to open the input file
            for reading.
        output_opener (Optional[FileOpener]): A callable to open the output
            file for writing.
        chunk_size (Optional[int]): The size of each chunk to read in bytes.
    """
    def __init__(
        self,
        pipeline: Pipeline,
        input_file: FilePath,
        output_file: FilePath,
        logger: Logger,
        input_opener: Optional[FileOpener] = xopen,
        output_opener: Optional[FileOpener] = xopen,
        chunk_size: Optional[int] = None,
    ):
        super().__init__(pipeline)
        self.input_file = input_file
        self.output_file = output_file
        self.logger = logger
        self.input_opener = input_opener
        self.output_opener = output_opener
        self.chunk_size = chunk_size

        self.total_processed = 0
        self.chunk_num = 0

    def log_progress(self):
        """
        Logs the total number of processed items.
        """
        self.logger.info(f'Total processed: {self.total_processed}')


class ParallelPipelineRunner(BasePipelineRunner):
    """
    A BasePipelineRunner subclass for running a read processing pipeline in
        parallel.

    This class extends the `BasePipelineRunner` base class and adds support
    for the parallel execution of a read processing pipeline. It makes use of
    multiple worker processes to distribute the workload, allowing for faster
    processing.

    Parameters:
        pipeline (Pipeline): The pipeline object to be run.
        processes (int): The number of parallel worker processes to use.
        **kwargs: Additional keyword arguments to be passed to the base class
            constructor.
    """
    def __init__(self, pipeline: Pipeline, processes: int, **kwargs):
        super().__init__(pipeline=pipeline, **kwargs)
        self.processes = processes

        self.work_queue = Queue()
        self.error_queue = Queue()
        self.task_conns = []
        self.result_conns = []

        self.workers = []
        self.fastx_reader = None
        self.chunk_writer = None

    def run(self):
        """
        Runs the pipeline in parallel.

        This method starts the reader, writer and worker processes. It then
        continuously retrieves items from the error queue and processes them
        until a sentinel value is encountered. Exceptions are raised if
        encountered and progress is logged after each processed item.
        """
        self.start_workers()
        self.start_writer()
        self.start_reader()

        while True:
            queue_item = self.error_queue.get()

            if queue_item is None:
                break

            if isinstance(queue_item, tuple):
                chunk_num, count = queue_item

                self.total_processed += count
                self.chunk_num = chunk_num

                self.log_progress()
                continue

            try:
                raise pickle.loads(queue_item)
            except Exception:
                self.logger.exception("Pipeline exception occurred:")
                raise

        for worker in self.workers:
            worker.join()

        self.fastx_reader.join()
        self.chunk_writer.join()

    def start_workers(self):
        """
        Starts the worker processes.

        This method creates the task and result connections for inter-process
        communication. It then starts the worker processes and makes them
        daemonic. The connections created for sending chunks of reads to the
        workers are added to a list of task connections. And the connections
        for receiving chunks of processed data are added to a list of result
        connections to be waited on by the writer process. 

        The writable end of each result pipe is closed to be sure that each
        worker process is only process that owns a handle for it. This ensures
        that when the worker closes its handle, `wait()` will promptly report
        the readable end as being ready.
        """
        for worker_index in range(self.processes):
            task_recv_conn, task_send_conn = Pipe(duplex=False)
            result_recv_conn, result_send_conn = Pipe(duplex=False)

            fastx_worker = FastxWorker(
                worker_index,
                self.pipeline,
                task_recv_conn,
                result_send_conn,
                self.work_queue,
                self.error_queue,
            )
            fastx_worker.daemon = True
            fastx_worker.start()

            result_send_conn.close()

            self.task_conns.append(task_send_conn)
            self.result_conns.append(result_recv_conn)
            self.workers.append(fastx_worker)

    def start_reader(self):
        """
        Starts the reader process.

        This method creates a FastxReader object and starts it as a daemonic
        process. The FastxReader is responsible for reading complete chunks of
        FASTA or FASTQ records from the input file, and for dispatching these
        chunks to the input connections of worker processes.
        """
        fastx_reader = FastxReader(
            self.input_file,
            self.task_conns,
            self.work_queue,
            self.error_queue,
            self.input_opener,
            self.chunk_size,
        )
        fastx_reader.daemon = True
        fastx_reader.start()

        self.fastx_reader = fastx_reader

    def start_writer(self):
        """
        Starts the writer process.

        This method creates an OrderedChunkWriter object and starts it as a
        daemonic process. The OrderedChunkWriter is responsible for receiving
        chunks of processed data from one or more workers and writing them to
        the output file in the correct order.
        """
        chunk_writer = OrderedChunkWriter(
            self.output_file,
            self.result_conns,
            self.error_queue,
            self.output_opener,
        )
        chunk_writer.daemon = True
        chunk_writer.start()

        self.chunk_writer = chunk_writer


class SerialPipelineRunner(BasePipelineRunner):
    """
    A BasePipelineRunner subclass for running a read processing pipeline.

    This class extends the `BasePipelineRunner` base class and runs a read
    processing pipeline using a serial execution approach. It reads the input
    file in chunks and processes each chunk sequentially.

    Parameters:
        pipeline (Pipeline): The pipeline object to be run.
        **kwargs: Additional keyword arguments to be passed to the base class
            constructor.
    """
    def __init__(self, pipeline: Pipeline, **kwargs):
        super().__init__(pipeline=pipeline, **kwargs)

    def run(self):
        with (
            self.output_opener(self.output_file, 'wb') as outfile,
            self.input_opener(self.input_file, 'rb') as infile,
        ):
            if self.chunk_size:
                chunks = dnaio.read_chunks(infile, self.chunk_size)
            else:
                chunks = dnaio.read_chunks(infile)

            for chunk in chunks:
                infile = io.BytesIO(chunk)
                with dnaio.open(infile) as reader:
                    result = self.pipeline.run(reader)
                    outfile.write(result)

                self.total_processed += reader.number_of_records
                self.chunk_num += 1

                self.log_progress()


class PipelineRunnerFactory:
    """
    Factory class for creating pipeline runners.
    """
    @staticmethod
    def create_runner(
        pipeline: Pipeline,
        processes: int,
        **kwargs,
    ) -> PipelineRunner:
        """
        Determines whether to create a parallel or serial pipeline runner based
        on the number of processes specified.

        Parameters:
            pipeline (Pipeline): The pipeline object to be run.
            processes (int): The number of parallel worker processes to use
                for parallel execution
            **kwargs: Additional keyword arguments to be passed to the pipeline
                runner constructor.
        """
        if processes > 1:
            return ParallelPipelineRunner(
                pipeline=pipeline,
                processes=processes,
                **kwargs,
            )
        else:
            return SerialPipelineRunner(
                pipeline=pipeline,
                **kwargs,
            )

