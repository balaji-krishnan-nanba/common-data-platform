# Performance Monitoring and Optimization Guide

This guide covers comprehensive performance monitoring, optimization strategies, and best practices for the Common Data Platform framework to ensure optimal throughput and resource utilization.

## Performance Monitoring Overview

The framework provides multi-layered performance monitoring:

1. **Infrastructure Performance** - Cluster utilization and resource metrics
2. **Pipeline Performance** - Data processing throughput and execution times
3. **Query Performance** - SQL query optimization and response times
4. **Storage Performance** - I/O patterns and Delta Lake optimization
5. **Cost Optimization** - Resource usage and cost-effectiveness metrics

## Infrastructure Performance Monitoring

### Cluster Metrics Dashboard

#### Real-time Cluster Monitoring
```sql
-- Cluster utilization metrics
CREATE OR REPLACE VIEW cluster_performance_metrics AS
WITH cluster_metrics AS (
    SELECT 
        cluster_id,
        cluster_name,
        timestamp,
        cpu_utilization_percent,
        memory_utilization_percent,
        disk_utilization_percent,
        network_in_mbps,
        network_out_mbps,
        active_tasks,
        completed_tasks,
        failed_tasks
    FROM system.compute.cluster_metrics
    WHERE timestamp >= current_timestamp() - INTERVAL 24 HOURS
),
aggregated_metrics AS (
    SELECT 
        cluster_id,
        cluster_name,
        AVG(cpu_utilization_percent) as avg_cpu_usage,
        MAX(cpu_utilization_percent) as peak_cpu_usage,
        AVG(memory_utilization_percent) as avg_memory_usage,
        MAX(memory_utilization_percent) as peak_memory_usage,
        AVG(disk_utilization_percent) as avg_disk_usage,
        SUM(completed_tasks) as total_completed_tasks,
        SUM(failed_tasks) as total_failed_tasks,
        COUNT(*) as measurement_count
    FROM cluster_metrics
    GROUP BY cluster_id, cluster_name
)
SELECT 
    cluster_name,
    ROUND(avg_cpu_usage, 2) as avg_cpu_percent,
    ROUND(peak_cpu_usage, 2) as peak_cpu_percent,
    ROUND(avg_memory_usage, 2) as avg_memory_percent,
    ROUND(peak_memory_usage, 2) as peak_memory_percent,
    ROUND(avg_disk_usage, 2) as avg_disk_percent,
    total_completed_tasks,
    total_failed_tasks,
    ROUND(total_failed_tasks::float / NULLIF(total_completed_tasks + total_failed_tasks, 0) * 100, 2) as failure_rate_percent,
    CASE 
        WHEN avg_cpu_usage > 90 OR avg_memory_usage > 90 THEN 'OVERUTILIZED'
        WHEN avg_cpu_usage < 30 AND avg_memory_usage < 30 THEN 'UNDERUTILIZED'
        ELSE 'OPTIMAL'
    END as utilization_status
FROM aggregated_metrics
ORDER BY avg_cpu_usage DESC;
```

#### Resource Optimization Recommendations
```python
class ClusterOptimizer:
    def __init__(self):
        self.utilization_thresholds = {
            'cpu_high': 85,
            'cpu_low': 20,
            'memory_high': 85,
            'memory_low': 20,
            'optimal_range': (40, 80)
        }
    
    def analyze_cluster_performance(self, cluster_id: str, days: int = 7) -> ClusterAnalysis:
        """Analyze cluster performance and provide optimization recommendations."""
        
        # Get cluster metrics
        metrics = self._get_cluster_metrics(cluster_id, days)
        
        # Calculate performance statistics
        stats = self._calculate_performance_stats(metrics)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(stats)
        
        # Calculate potential cost savings
        cost_analysis = self._calculate_cost_impact(stats, recommendations)
        
        return ClusterAnalysis(
            cluster_id=cluster_id,
            analysis_period_days=days,
            performance_stats=stats,
            recommendations=recommendations,
            cost_analysis=cost_analysis
        )
    
    def _calculate_performance_stats(self, metrics: List[Dict]) -> PerformanceStats:
        """Calculate detailed performance statistics."""
        cpu_values = [m['cpu_utilization'] for m in metrics]
        memory_values = [m['memory_utilization'] for m in metrics]
        
        return PerformanceStats(
            avg_cpu_utilization=statistics.mean(cpu_values),
            p95_cpu_utilization=numpy.percentile(cpu_values, 95),
            avg_memory_utilization=statistics.mean(memory_values),
            p95_memory_utilization=numpy.percentile(memory_values, 95),
            idle_time_percentage=self._calculate_idle_time(metrics),
            peak_utilization_hours=self._identify_peak_hours(metrics),
            utilization_variance=statistics.stdev(cpu_values)
        )
    
    def _generate_recommendations(self, stats: PerformanceStats) -> List[Recommendation]:
        """Generate optimization recommendations based on performance stats."""
        recommendations = []
        
        # CPU optimization
        if stats.avg_cpu_utilization < self.utilization_thresholds['cpu_low']:
            recommendations.append(Recommendation(
                type='DOWNSIZE',
                category='CPU',
                description='CPU utilization is consistently low. Consider downsizing cluster.',
                impact='COST_SAVINGS',
                estimated_savings=self._estimate_cpu_savings(stats),
                implementation='Reduce worker node count or use smaller instance types'
            ))
        elif stats.p95_cpu_utilization > self.utilization_thresholds['cpu_high']:
            recommendations.append(Recommendation(
                type='UPSIZE',
                category='CPU',
                description='CPU utilization peaks are high. Consider increasing capacity.',
                impact='PERFORMANCE_IMPROVEMENT',
                estimated_improvement='15-30% faster processing',
                implementation='Add worker nodes or use larger instance types'
            ))
        
        # Memory optimization
        if stats.avg_memory_utilization < self.utilization_thresholds['memory_low']:
            recommendations.append(Recommendation(
                type='OPTIMIZE',
                category='MEMORY',
                description='Memory utilization is low. Consider memory-optimized instances.',
                impact='COST_SAVINGS',
                implementation='Switch to compute-optimized instances'
            ))
        
        # Auto-scaling recommendations
        if stats.utilization_variance > 30:
            recommendations.append(Recommendation(
                type='AUTOSCALING',
                category='ELASTICITY',
                description='High utilization variance detected. Enable auto-scaling.',
                impact='COST_AND_PERFORMANCE',
                implementation='Configure auto-scaling with min/max worker bounds'
            ))
        
        return recommendations
```

### Spark Performance Monitoring

#### Query Execution Analysis
```python
class SparkPerformanceMonitor:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.performance_metrics = {}
    
    def monitor_query_performance(self, query_id: str) -> QueryPerformanceMetrics:
        """Monitor individual query performance metrics."""
        
        # Get query execution metrics
        execution_plan = self.spark.sql(f"EXPLAIN EXTENDED {query_id}").collect()
        
        # Get Spark UI metrics
        spark_context = self.spark.sparkContext
        status_tracker = spark_context.statusTracker()
        
        # Collect job metrics
        active_jobs = status_tracker.getActiveJobsIds()
        job_infos = [status_tracker.getJobInfo(job_id) for job_id in active_jobs]
        
        # Calculate performance metrics
        metrics = QueryPerformanceMetrics(
            query_id=query_id,
            execution_time_ms=self._get_execution_time(query_id),
            records_processed=self._get_records_processed(query_id),
            bytes_read=self._get_bytes_read(query_id),
            bytes_written=self._get_bytes_written(query_id),
            shuffle_read_bytes=self._get_shuffle_metrics(query_id)['read'],
            shuffle_write_bytes=self._get_shuffle_metrics(query_id)['write'],
            cache_hit_ratio=self._get_cache_hit_ratio(query_id),
            spill_metrics=self._get_spill_metrics(query_id)
        )
        
        return metrics
    
    def identify_performance_bottlenecks(self, metrics: QueryPerformanceMetrics) -> List[PerformanceBottleneck]:
        """Identify performance bottlenecks in query execution."""
        bottlenecks = []
        
        # High shuffle volumes
        if metrics.shuffle_read_bytes > 1_000_000_000:  # 1GB
            bottlenecks.append(PerformanceBottleneck(
                type='HIGH_SHUFFLE',
                severity='HIGH',
                description=f'High shuffle read volume: {metrics.shuffle_read_bytes / 1e9:.2f}GB',
                recommendations=[
                    'Consider broadcast joins for smaller tables',
                    'Optimize partition keys to reduce shuffling',
                    'Use bucketing for frequently joined tables'
                ]
            ))
        
        # Low cache hit ratio
        if metrics.cache_hit_ratio < 0.5:
            bottlenecks.append(PerformanceBottleneck(
                type='LOW_CACHE_EFFICIENCY',
                severity='MEDIUM',
                description=f'Low cache hit ratio: {metrics.cache_hit_ratio:.2%}',
                recommendations=[
                    'Cache frequently accessed DataFrames',
                    'Optimize cache storage levels',
                    'Consider persisting intermediate results'
                ]
            ))
        
        # Memory spilling
        if metrics.spill_metrics['memory_spill'] > 0:
            bottlenecks.append(PerformanceBottleneck(
                type='MEMORY_SPILL',
                severity='HIGH',
                description=f'Memory spilling detected: {metrics.spill_metrics["memory_spill"] / 1e6:.2f}MB',
                recommendations=[
                    'Increase executor memory',
                    'Optimize data types and schema',
                    'Reduce partition size',
                    'Enable adaptive query execution'
                ]
            ))
        
        return bottlenecks
```

## Pipeline Performance Optimization

### Data Processing Throughput
```sql
-- Pipeline throughput analysis
CREATE OR REPLACE VIEW pipeline_throughput_analysis AS
WITH pipeline_metrics AS (
    SELECT 
        source_name,
        DATE(execution_start_time) as execution_date,
        execution_start_time,
        execution_end_time,
        records_processed,
        files_processed,
        DATEDIFF(SECOND, execution_start_time, execution_end_time) as duration_seconds,
        records_processed / NULLIF(DATEDIFF(SECOND, execution_start_time, execution_end_time), 0) as records_per_second,
        status
    FROM `cddp-dev-bronze`.`system`.`pipeline_executions`
    WHERE execution_start_time >= current_date() - INTERVAL 30 DAYS
        AND status = 'success'
),
daily_aggregates AS (
    SELECT 
        source_name,
        execution_date,
        COUNT(*) as daily_executions,
        SUM(records_processed) as daily_records,
        SUM(files_processed) as daily_files,
        AVG(duration_seconds) as avg_duration_seconds,
        AVG(records_per_second) as avg_throughput_rps,
        MAX(records_per_second) as peak_throughput_rps,
        MIN(records_per_second) as min_throughput_rps
    FROM pipeline_metrics
    GROUP BY source_name, execution_date
),
performance_trends AS (
    SELECT 
        source_name,
        execution_date,
        avg_throughput_rps,
        LAG(avg_throughput_rps, 1) OVER (
            PARTITION BY source_name 
            ORDER BY execution_date
        ) as prev_day_throughput,
        LAG(avg_throughput_rps, 7) OVER (
            PARTITION BY source_name 
            ORDER BY execution_date
        ) as week_ago_throughput
    FROM daily_aggregates
)
SELECT 
    da.source_name,
    da.execution_date,
    da.daily_executions,
    da.daily_records,
    da.daily_files,
    ROUND(da.avg_duration_seconds / 60, 2) as avg_duration_minutes,
    ROUND(da.avg_throughput_rps, 2) as avg_records_per_second,
    ROUND(da.peak_throughput_rps, 2) as peak_records_per_second,
    
    -- Performance trends
    ROUND((pt.avg_throughput_rps - pt.prev_day_throughput) / NULLIF(pt.prev_day_throughput, 0) * 100, 2) as daily_change_percent,
    ROUND((pt.avg_throughput_rps - pt.week_ago_throughput) / NULLIF(pt.week_ago_throughput, 0) * 100, 2) as weekly_change_percent,
    
    -- Performance classification
    CASE 
        WHEN da.avg_throughput_rps > 1000 THEN 'EXCELLENT'
        WHEN da.avg_throughput_rps > 500 THEN 'GOOD'
        WHEN da.avg_throughput_rps > 100 THEN 'ACCEPTABLE'
        ELSE 'NEEDS_OPTIMIZATION'
    END as performance_rating
    
FROM daily_aggregates da
JOIN performance_trends pt ON da.source_name = pt.source_name AND da.execution_date = pt.execution_date
ORDER BY da.source_name, da.execution_date DESC;
```

### Adaptive Performance Tuning
```python
class AdaptivePerformanceTuner:
    def __init__(self):
        self.performance_history = {}
        self.optimization_strategies = {
            'high_volume': self._optimize_for_volume,
            'low_latency': self._optimize_for_latency,
            'cost_efficiency': self._optimize_for_cost,
            'balanced': self._optimize_balanced
        }
    
    def auto_tune_pipeline(self, source_name: str, performance_data: PerformanceData) -> TuningRecommendations:
        """Automatically tune pipeline based on performance characteristics."""
        
        # Analyze current performance
        analysis = self._analyze_performance_profile(performance_data)
        
        # Determine optimization strategy
        strategy = self._select_optimization_strategy(analysis)
        
        # Generate specific recommendations
        recommendations = self.optimization_strategies[strategy](analysis)
        
        # Validate recommendations
        validated_recommendations = self._validate_recommendations(recommendations, performance_data)
        
        return TuningRecommendations(
            source_name=source_name,
            strategy=strategy,
            current_performance=analysis,
            recommendations=validated_recommendations,
            expected_improvement=self._estimate_improvement(validated_recommendations)
        )
    
    def _optimize_for_volume(self, analysis: PerformanceAnalysis) -> List[OptimizationRecommendation]:
        """Optimize for high-volume data processing."""
        recommendations = []
        
        # Cluster sizing
        if analysis.avg_throughput < analysis.target_throughput:
            recommendations.append(OptimizationRecommendation(
                category='CLUSTER_SIZING',
                action='INCREASE_WORKERS',
                parameters={
                    'current_workers': analysis.current_workers,
                    'recommended_workers': min(analysis.current_workers * 2, 32),
                    'node_type': 'Standard_DS4_v2'  # Memory optimized for large datasets
                },
                expected_impact='2x throughput improvement'
            ))
        
        # Spark configuration
        recommendations.append(OptimizationRecommendation(
            category='SPARK_CONFIG',
            action='OPTIMIZE_FOR_THROUGHPUT',
            parameters={
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.minPartitionNum': '1',
                'spark.sql.adaptive.coalescePartitions.initialPartitionNum': str(analysis.current_workers * 4),
                'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
                'spark.sql.execution.arrow.pyspark.enabled': 'true'
            }
        ))
        
        # Partitioning strategy
        if analysis.partition_count < analysis.current_workers * 2:
            recommendations.append(OptimizationRecommendation(
                category='PARTITIONING',
                action='INCREASE_PARTITIONS',
                parameters={
                    'target_partition_count': analysis.current_workers * 4,
                    'partition_strategy': 'hash_based',
                    'partition_columns': analysis.recommended_partition_columns
                }
            ))
        
        return recommendations
    
    def _optimize_for_latency(self, analysis: PerformanceAnalysis) -> List[OptimizationRecommendation]:
        """Optimize for low-latency processing."""
        recommendations = []
        
        # Use faster instance types
        recommendations.append(OptimizationRecommendation(
            category='CLUSTER_SIZING',
            action='USE_COMPUTE_OPTIMIZED',
            parameters={
                'node_type': 'Standard_F8s_v2',  # Compute optimized
                'enable_local_ssd': True,
                'instance_pool_id': 'fast-startup-pool'
            }
        ))
        
        # Cache frequently accessed data
        recommendations.append(OptimizationRecommendation(
            category='CACHING',
            action='ENABLE_AGGRESSIVE_CACHING',
            parameters={
                'cache_level': 'MEMORY_AND_DISK_SER',
                'cache_tables': analysis.frequently_accessed_tables,
                'cache_size_gb': min(analysis.available_memory * 0.6, 100)
            }
        ))
        
        # Optimize query execution
        recommendations.append(OptimizationRecommendation(
            category='QUERY_OPTIMIZATION',
            action='ENABLE_VECTORIZATION',
            parameters={
                'spark.sql.execution.arrow.pyspark.enabled': 'true',
                'spark.sql.execution.arrow.maxRecordsPerBatch': '20000',
                'spark.sql.execution.arrow.pyspark.fallback.enabled': 'false'
            }
        ))
        
        return recommendations
```

## Delta Lake Performance Optimization

### Table Optimization Strategies
```python
class DeltaTableOptimizer:
    def __init__(self):
        self.optimization_thresholds = {
            'file_count_high': 1000,
            'file_size_small_mb': 10,
            'file_size_large_mb': 1000,
            'version_count_high': 100
        }
    
    def optimize_table_performance(self, table_name: str) -> TableOptimizationPlan:
        """Create comprehensive table optimization plan."""
        
        # Analyze table structure and usage
        table_analysis = self._analyze_table(table_name)
        
        # Generate optimization plan
        optimization_plan = self._create_optimization_plan(table_analysis)
        
        # Estimate impact
        impact_estimate = self._estimate_optimization_impact(table_analysis, optimization_plan)
        
        return TableOptimizationPlan(
            table_name=table_name,
            current_state=table_analysis,
            optimization_actions=optimization_plan,
            estimated_impact=impact_estimate
        )
    
    def _analyze_table(self, table_name: str) -> TableAnalysis:
        """Analyze Delta table for optimization opportunities."""
        
        # Get table details
        table_details = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
        
        # Get table history
        history_df = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 20")
        
        # Analyze file structure
        files_df = spark.sql(f"""
            SELECT 
                COUNT(*) as file_count,
                AVG(size) / 1024 / 1024 as avg_file_size_mb,
                MIN(size) / 1024 / 1024 as min_file_size_mb,
                MAX(size) / 1024 / 1024 as max_file_size_mb,
                SUM(size) / 1024 / 1024 / 1024 as total_size_gb
            FROM (
                SELECT input_file_name(), COUNT(*) as records, 
                       LENGTH(input_file_name()) as size
                FROM {table_name}
                GROUP BY input_file_name()
            )
        """).collect()[0]
        
        # Check for small files
        small_files = spark.sql(f"""
            SELECT COUNT(*) as small_file_count
            FROM (
                SELECT input_file_name(), COUNT(*) as records
                FROM {table_name}
                GROUP BY input_file_name()
                HAVING COUNT(*) < 10000
            )
        """).collect()[0]['small_file_count']
        
        return TableAnalysis(
            table_name=table_name,
            total_size_gb=files_df['total_size_gb'],
            file_count=files_df['file_count'],
            avg_file_size_mb=files_df['avg_file_size_mb'],
            small_file_count=small_files,
            version_count=history_df.count(),
            partition_columns=table_details['partitionColumns'],
            last_optimized=self._get_last_optimization_date(table_name)
        )
    
    def _create_optimization_plan(self, analysis: TableAnalysis) -> List[OptimizationAction]:
        """Create specific optimization actions based on analysis."""
        actions = []
        
        # File compaction
        if (analysis.small_file_count > 100 or 
            analysis.avg_file_size_mb < self.optimization_thresholds['file_size_small_mb']):
            actions.append(OptimizationAction(
                action_type='OPTIMIZE',
                priority='HIGH',
                description='Compact small files to improve query performance',
                sql_command=f"OPTIMIZE {analysis.table_name}",
                estimated_duration_minutes=analysis.total_size_gb * 2  # Rough estimate
            ))
        
        # Z-ordering
        if analysis.partition_columns and len(analysis.partition_columns) > 0:
            # Recommend Z-ordering on frequently queried columns
            zorder_columns = self._identify_zorder_columns(analysis.table_name)
            if zorder_columns:
                actions.append(OptimizationAction(
                    action_type='ZORDER',
                    priority='MEDIUM',
                    description=f'Z-order by {", ".join(zorder_columns)} for better query pruning',
                    sql_command=f"OPTIMIZE {analysis.table_name} ZORDER BY ({', '.join(zorder_columns)})",
                    estimated_duration_minutes=analysis.total_size_gb * 3
                ))
        
        # Vacuum old versions
        if analysis.version_count > self.optimization_thresholds['version_count_high']:
            actions.append(OptimizationAction(
                action_type='VACUUM',
                priority='LOW',
                description='Remove old file versions to reduce storage costs',
                sql_command=f"VACUUM {analysis.table_name} RETAIN 168 HOURS",  # 7 days
                estimated_duration_minutes=analysis.total_size_gb * 0.5
            ))
        
        # Auto-optimize settings
        actions.append(OptimizationAction(
            action_type='AUTO_OPTIMIZE',
            priority='MEDIUM',
            description='Enable auto-optimization for future writes',
            sql_command=f"""
                ALTER TABLE {analysis.table_name} 
                SET TBLPROPERTIES (
                    'delta.autoOptimize.optimizeWrite' = 'true',
                    'delta.autoOptimize.autoCompact' = 'true'
                )
            """,
            estimated_duration_minutes=1
        ))
        
        return actions
```

### Query Performance Analysis
```sql
-- Query performance analysis with optimization suggestions
CREATE OR REPLACE VIEW query_performance_analysis AS
WITH query_metrics AS (
    SELECT 
        query_id,
        query_text,
        warehouse_id,
        user_name,
        start_time,
        end_time,
        DATEDIFF(SECOND, start_time, end_time) as duration_seconds,
        total_task_duration_ms / 1000 as total_task_duration_seconds,
        rows_read,
        bytes_read,
        rows_written,
        bytes_written,
        peak_memory_usage,
        spilled_local_bytes,
        spilled_remote_bytes
    FROM system.query.history
    WHERE start_time >= current_date() - INTERVAL 7 DAYS
        AND state = 'FINISHED'
        AND warehouse_id IS NOT NULL
),
performance_stats AS (
    SELECT 
        query_id,
        duration_seconds,
        total_task_duration_seconds,
        rows_read,
        bytes_read / 1024 / 1024 as mb_read,
        CASE 
            WHEN duration_seconds > 0 THEN rows_read / duration_seconds 
            ELSE 0 
        END as rows_per_second,
        CASE 
            WHEN duration_seconds > 0 THEN (bytes_read / 1024 / 1024) / duration_seconds 
            ELSE 0 
        END as mb_per_second,
        peak_memory_usage / 1024 / 1024 as peak_memory_mb,
        (spilled_local_bytes + spilled_remote_bytes) / 1024 / 1024 as total_spill_mb
    FROM query_metrics
),
performance_classification AS (
    SELECT 
        *,
        CASE 
            WHEN duration_seconds > 3600 THEN 'VERY_SLOW'
            WHEN duration_seconds > 900 THEN 'SLOW'
            WHEN duration_seconds > 300 THEN 'MODERATE'
            WHEN duration_seconds > 60 THEN 'FAST'
            ELSE 'VERY_FAST'
        END as speed_category,
        CASE 
            WHEN total_spill_mb > 1000 THEN 'HIGH_SPILL'
            WHEN total_spill_mb > 100 THEN 'MODERATE_SPILL'
            WHEN total_spill_mb > 0 THEN 'LOW_SPILL'
            ELSE 'NO_SPILL'
        END as spill_category,
        CASE 
            WHEN rows_per_second > 10000 THEN 'HIGH_THROUGHPUT'
            WHEN rows_per_second > 1000 THEN 'GOOD_THROUGHPUT'
            WHEN rows_per_second > 100 THEN 'MODERATE_THROUGHPUT'
            ELSE 'LOW_THROUGHPUT'
        END as throughput_category
    FROM performance_stats
)
SELECT 
    qm.query_id,
    LEFT(qm.query_text, 100) as query_text_preview,
    qm.user_name,
    qm.start_time,
    pc.duration_seconds,
    pc.rows_read,
    ROUND(pc.mb_read, 2) as mb_read,
    ROUND(pc.rows_per_second, 2) as rows_per_second,
    ROUND(pc.mb_per_second, 2) as mb_per_second,
    pc.speed_category,
    pc.throughput_category,
    pc.spill_category,
    ROUND(pc.peak_memory_mb, 2) as peak_memory_mb,
    ROUND(pc.total_spill_mb, 2) as total_spill_mb,
    
    -- Optimization suggestions
    CASE 
        WHEN pc.speed_category IN ('SLOW', 'VERY_SLOW') AND pc.total_spill_mb > 100 THEN 'Increase memory allocation'
        WHEN pc.speed_category IN ('SLOW', 'VERY_SLOW') AND pc.throughput_category = 'LOW_THROUGHPUT' THEN 'Optimize query or add indexes'
        WHEN pc.spill_category = 'HIGH_SPILL' THEN 'Increase executor memory or optimize data types'
        WHEN pc.throughput_category = 'LOW_THROUGHPUT' AND pc.mb_read > 1000 THEN 'Consider partitioning or filtering'
        ELSE 'Performance acceptable'
    END as optimization_suggestion
    
FROM query_metrics qm
JOIN performance_classification pc ON qm.query_id = pc.query_id
ORDER BY pc.duration_seconds DESC;
```

## Cost Performance Optimization

### Cost Analysis and Optimization
```python
class CostPerformanceOptimizer:
    def __init__(self):
        self.cost_metrics = {
            'compute_cost_per_hour': 0.50,  # Example rate
            'storage_cost_per_gb_month': 0.023,
            'data_transfer_cost_per_gb': 0.09
        }
    
    def analyze_cost_performance(self, period_days: int = 30) -> CostPerformanceAnalysis:
        """Analyze cost vs performance trade-offs."""
        
        # Get resource usage data
        usage_data = self._get_resource_usage(period_days)
        
        # Calculate costs
        cost_breakdown = self._calculate_costs(usage_data)
        
        # Analyze efficiency
        efficiency_metrics = self._calculate_efficiency_metrics(usage_data, cost_breakdown)
        
        # Generate optimization recommendations
        cost_optimizations = self._generate_cost_optimizations(efficiency_metrics)
        
        return CostPerformanceAnalysis(
            period_days=period_days,
            total_cost=cost_breakdown['total'],
            cost_breakdown=cost_breakdown,
            efficiency_metrics=efficiency_metrics,
            optimizations=cost_optimizations
        )
    
    def _calculate_efficiency_metrics(self, usage_data: Dict, cost_breakdown: Dict) -> EfficiencyMetrics:
        """Calculate cost efficiency metrics."""
        
        # Cost per record processed
        total_records = sum(usage_data['daily_records_processed'])
        cost_per_record = cost_breakdown['total'] / total_records if total_records > 0 else 0
        
        # Cost per GB processed
        total_gb_processed = sum(usage_data['daily_gb_processed'])
        cost_per_gb = cost_breakdown['total'] / total_gb_processed if total_gb_processed > 0 else 0
        
        # Resource utilization efficiency
        avg_cpu_utilization = statistics.mean(usage_data['cpu_utilization_readings'])
        avg_memory_utilization = statistics.mean(usage_data['memory_utilization_readings'])
        
        utilization_efficiency = (avg_cpu_utilization + avg_memory_utilization) / 2
        
        # Cost trend analysis
        daily_costs = usage_data['daily_costs']
        cost_trend = self._calculate_trend(daily_costs)
        
        return EfficiencyMetrics(
            cost_per_record=cost_per_record,
            cost_per_gb_processed=cost_per_gb,
            resource_utilization_efficiency=utilization_efficiency,
            cost_trend_percentage=cost_trend,
            peak_cost_day=max(daily_costs),
            lowest_cost_day=min(daily_costs)
        )
    
    def _generate_cost_optimizations(self, efficiency_metrics: EfficiencyMetrics) -> List[CostOptimization]:
        """Generate cost optimization recommendations."""
        optimizations = []
        
        # Low utilization optimization
        if efficiency_metrics.resource_utilization_efficiency < 0.4:
            optimizations.append(CostOptimization(
                type='RIGHTSIZING',
                title='Reduce cluster size due to low utilization',
                description=f'Resource utilization is only {efficiency_metrics.resource_utilization_efficiency:.1%}',
                estimated_monthly_savings=self._estimate_rightsizing_savings(efficiency_metrics),
                implementation_complexity='LOW',
                recommendations=[
                    'Reduce worker node count by 30-50%',
                    'Switch to smaller instance types',
                    'Enable auto-scaling with lower minimum'
                ]
            ))
        
        # Spot instance optimization
        optimizations.append(CostOptimization(
            type='SPOT_INSTANCES',
            title='Use spot instances for non-critical workloads',
            description='Leverage spot instances for development and testing',
            estimated_monthly_savings=self._estimate_spot_savings(),
            implementation_complexity='MEDIUM',
            recommendations=[
                'Configure spot instances for dev/test clusters',
                'Use fault-tolerant job designs',
                'Implement checkpointing for long-running jobs'
            ]
        ))
        
        # Storage optimization
        if efficiency_metrics.cost_per_gb_processed > 0.10:
            optimizations.append(CostOptimization(
                type='STORAGE_OPTIMIZATION',
                title='Optimize storage costs through lifecycle management',
                description='High storage processing costs detected',
                estimated_monthly_savings=self._estimate_storage_savings(),
                implementation_complexity='LOW',
                recommendations=[
                    'Implement data lifecycle policies',
                    'Archive old data to cheaper storage tiers',
                    'Optimize file formats (Delta Lake compression)',
                    'Regular vacuum operations to clean up old versions'
                ]
            ))
        
        return optimizations
```

## Automated Performance Tuning

### Self-Optimizing Pipeline Framework
```python
class AutoTuningFramework:
    def __init__(self):
        self.tuning_history = {}
        self.performance_baseline = {}
        self.auto_tuning_enabled = True
    
    def enable_auto_tuning(self, source_name: str):
        """Enable automatic performance tuning for a pipeline."""
        
        # Establish performance baseline
        baseline = self._establish_baseline(source_name)
        self.performance_baseline[source_name] = baseline
        
        # Start monitoring
        self._start_continuous_monitoring(source_name)
        
        logger.info(f"Auto-tuning enabled for {source_name}")
    
    def _continuous_tuning_loop(self, source_name: str):
        """Continuous monitoring and tuning loop."""
        
        while self.auto_tuning_enabled:
            try:
                # Collect recent performance data
                recent_performance = self._collect_performance_data(source_name, hours=24)
                
                # Compare against baseline
                performance_deviation = self._analyze_deviation(recent_performance, self.performance_baseline[source_name])
                
                # Apply tuning if needed
                if performance_deviation.requires_tuning:
                    tuning_actions = self._generate_tuning_actions(performance_deviation)
                    self._apply_tuning_actions(source_name, tuning_actions)
                    
                    # Update baseline if improvement confirmed
                    if self._validate_improvement(source_name, tuning_actions):
                        self._update_baseline(source_name, recent_performance)
                
                # Sleep before next iteration
                time.sleep(3600)  # Check every hour
                
            except Exception as e:
                logger.error(f"Auto-tuning error for {source_name}: {str(e)}")
                time.sleep(3600)  # Continue after error
    
    def _apply_tuning_actions(self, source_name: str, actions: List[TuningAction]):
        """Apply performance tuning actions."""
        
        for action in actions:
            try:
                if action.action_type == 'CLUSTER_RESIZE':
                    self._resize_cluster(source_name, action.parameters)
                elif action.action_type == 'SPARK_CONFIG_UPDATE':
                    self._update_spark_config(source_name, action.parameters)
                elif action.action_type == 'PARTITION_OPTIMIZATION':
                    self._optimize_partitioning(source_name, action.parameters)
                
                # Log tuning action
                self._log_tuning_action(source_name, action)
                
            except Exception as e:
                logger.error(f"Failed to apply tuning action {action.action_type}: {str(e)}")
```

This comprehensive performance monitoring and optimization guide ensures that the Common Data Platform framework operates at peak efficiency while maintaining cost-effectiveness and meeting SLA requirements.