// Package main implements the backtesting service with gRPC API and Arrow IPC pipeline
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "backtest-root-installer/go-services/proto"
	"backtest-root-installer/go-services/services/arrowpipeline"
	"backtest-root-installer/go-services/services/clickhouse"
	"backtest-root-installer/go-services/services/config"
	"backtest-root-installer/go-services/services/engine"
	"backtest-root-installer/go-services/services/monitoring"
)

// BacktestService implements the gRPC backtesting service
type BacktestService struct {
	pb.UnimplementedBacktestServiceServer
	engine        *engine.EngineClient
	clickhouse    *clickhouse.Client
	arrowPipeline *arrowpipeline.Pipeline
	monitoring    *monitoring.Metrics
	logger        *zap.Logger
	config        *config.Config
}

// NewBacktestService creates a new backtesting service
func NewBacktestService(cfg *config.Config, logger *zap.Logger) (*BacktestService, error) {
	// Initialize ClickHouse client
	chClient, err := clickhouse.NewClient(cfg.ClickHouse)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse client: %w", err)
	}

	// Initialize Arrow pipeline
	arrowConfig := &arrowpipeline.Config{
		BatchSize:      cfg.Arrow.BatchSize,
		PrefetchSize:   cfg.Arrow.PrefetchSize,
		Compression:    cfg.Arrow.Compression,
		MemoryPoolSize: cfg.Arrow.MemoryPoolSize,
	}
	arrowPipeline, err := arrowpipeline.NewPipeline(arrowConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow pipeline: %w", err)
	}

	// Initialize Rust engine client
	engineClient, err := engine.NewClient(cfg.Engine)
	if err != nil {
		return nil, fmt.Errorf("failed to create engine client: %w", err)
	}

	// Initialize monitoring
	metrics, err := monitoring.NewMetrics(cfg.Monitoring)
	if err != nil {
		return nil, fmt.Errorf("failed to create monitoring: %w", err)
	}

	return &BacktestService{
		engine:        engineClient,
		clickhouse:    chClient,
		arrowPipeline: arrowPipeline,
		monitoring:    metrics,
		logger:        logger,
		config:        cfg,
	}, nil
}

// ExecuteBacktest implements the gRPC ExecuteBacktest method
func (s *BacktestService) ExecuteBacktest(ctx context.Context, req *pb.BacktestRequest) (*pb.BacktestResponse, error) {
	startTime := time.Now()
	jobID := uuid.New().String()

	s.logger.Info("Starting backtest execution",
		zap.String("job_id", jobID),
		zap.Strings("symbols", req.Symbols),
		zap.String("timeframe", req.Timeframe),
		zap.Int64("start_time", req.StartTime),
		zap.Int64("end_time", req.EndTime),
	)

	// Convert request to internal job structure
	job := &engine.BacktestJob{
		JobID:            jobID,
		Symbols:          req.Symbols,
		Timeframe:        req.Timeframe,
		StartTime:        uint64(req.StartTime),
		EndTime:          uint64(req.EndTime),
		IntrabarPolicy:   s.convertIntrabarPolicy(req.IntrabarPolicy),
		FeeVersion:       req.FeeVersion,
		SlippageMode:     s.convertSlippageMode(req.SlippageMode),
		StrategyWasmHash: req.StrategyWasmHash,
		SnapshotID:       req.SnapshotId,
	}

	// Execute backtest
	result, err := s.executeBacktestJob(ctx, job)
	if err != nil {
		s.logger.Error("Backtest execution failed",
			zap.String("job_id", jobID),
			zap.Error(err),
		)
		return nil, err
	}

	executionTime := time.Since(startTime)
	s.logger.Info("Backtest completed",
		zap.String("job_id", jobID),
		zap.Duration("execution_time", executionTime),
		zap.Int("symbol_count", len(result.SymbolResults)),
	)

	// Convert result to gRPC response
	response := s.convertToGrpcResponse(result)
	return response, nil
}

// executeBacktestJob executes a backtest job with parallel symbol processing
func (s *BacktestService) executeBacktestJob(ctx context.Context, job *engine.BacktestJob) (*engine.BacktestResult, error) {
	// Create worker pool for parallel execution
	numWorkers := runtime.NumCPU()
	if s.config.Engine.MaxWorkers > 0 {
		numWorkers = s.config.Engine.MaxWorkers
	}

	s.logger.Info("Starting parallel backtest execution",
		zap.String("job_id", job.JobID),
		zap.Int("workers", numWorkers),
		zap.Int("symbols", len(job.Symbols)),
	)

	// Channel for distributing symbols to workers
	symbolChan := make(chan string, len(job.Symbols))
	resultChan := make(chan *engine.SymbolResult, len(job.Symbols))
	errorChan := make(chan error, len(job.Symbols))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go s.worker(ctx, i, job, symbolChan, resultChan, errorChan, &wg)
	}

	// Send symbols to workers
	for _, symbol := range job.Symbols {
		symbolChan <- symbol
	}
	close(symbolChan)

	// Wait for workers to complete
	go func() {
		wg.Wait()
		close(resultChan)
		close(errorChan)
	}()

	// Collect results
	var symbolResults []*engine.SymbolResult
	var errors []error

	for {
		select {
		case result, ok := <-resultChan:
			if !ok {
				goto done
			}
			symbolResults = append(symbolResults, result)

		case err, ok := <-errorChan:
			if !ok {
				goto done
			}
			errors = append(errors, err)
		}
	}

done:
	// Check for errors
	if len(errors) > 0 {
		return nil, fmt.Errorf("backtest execution failed: %v", errors)
	}

	// Create final result
	finalResult := &engine.BacktestResult{
		JobID:              job.JobID,
		ExecutionTimeMs:    uint64(time.Since(time.Now()).Milliseconds()),
		SymbolResults:      symbolResults,
		PerformanceMetrics: &engine.PerformanceMetrics{},
		Manifest:           s.createRunManifest(job),
	}

	return finalResult, nil
}

// worker processes symbols in parallel
func (s *BacktestService) worker(
	ctx context.Context,
	workerID int,
	job *engine.BacktestJob,
	symbolChan <-chan string,
	resultChan chan<- *engine.SymbolResult,
	errorChan chan<- error,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	for symbol := range symbolChan {
		s.logger.Debug("Worker processing symbol",
			zap.Int("worker_id", workerID),
			zap.String("symbol", symbol),
		)

		result, err := s.processSymbol(ctx, job, symbol)
		if err != nil {
			errorChan <- fmt.Errorf("failed to process symbol %s: %w", symbol, err)
			continue
		}

		resultChan <- result
	}
}

// processSymbol processes a single symbol
func (s *BacktestService) processSymbol(ctx context.Context, job *engine.BacktestJob, symbol string) (*engine.SymbolResult, error) {
	// Load market data from ClickHouse
	marketData, err := s.loadMarketData(ctx, symbol, job)
	if err != nil {
		return nil, fmt.Errorf("failed to load market data: %w", err)
	}

	// Stream data via Arrow IPC to Rust engine
	arrowData, err := s.arrowPipeline.ConvertToArrow(marketData)
	if err != nil {
		return nil, fmt.Errorf("failed to convert to Arrow: %w", err)
	}

	// Execute backtest on Rust engine
	result, err := s.engine.ExecuteSymbolBacktest(ctx, &engine.SymbolBacktestRequest{
		JobID:      job.JobID,
		Symbol:     symbol,
		ArrowData:  arrowData,
		Strategy:   job.StrategyWasmHash,
		Parameters: s.buildStrategyParameters(job),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to execute symbol backtest: %w", err)
	}

	return result, nil
}

// loadMarketData loads market data from ClickHouse
func (s *BacktestService) loadMarketData(ctx context.Context, symbol string, job *engine.BacktestJob) (*clickhouse.MarketData, error) {
	// Build query for market data
	query := `
		SELECT 
			symbol,
			ts,
			open,
			high,
			low,
			close,
			volume,
			trade_count
		FROM market.view_bars
		WHERE symbol = ? 
		AND ts >= ? 
		AND ts < ?
		AND snapshot_id = ?
		ORDER BY ts
	`

	// Execute query
	rows, err := s.clickhouse.Query(ctx, query, symbol, job.StartTime, job.EndTime, job.SnapshotID)
	if err != nil {
		return nil, fmt.Errorf("failed to query market data: %w", err)
	}
	defer rows.Close()

	// Parse results
	var marketData clickhouse.MarketData
	for rows.Next() {
		var bar clickhouse.Bar
		err := rows.Scan(
			&bar.Symbol,
			&bar.Timestamp,
			&bar.Open,
			&bar.High,
			&bar.Low,
			&bar.Close,
			&bar.Volume,
			&bar.TradeCount,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan bar data: %w", err)
		}
		marketData.Bars = append(marketData.Bars, bar)
	}

	return &marketData, nil
}

// Helper methods for type conversion
func (s *BacktestService) convertIntrabarPolicy(policy pb.IntrabarPolicy) engine.IntrabarPolicy {
	switch policy {
	case pb.IntrabarPolicy_EXACT_TRADES:
		return engine.IntrabarPolicyExactTrades
	case pb.IntrabarPolicy_ONE_SECOND_BARS:
		return engine.IntrabarPolicyOneSecondBars
	case pb.IntrabarPolicy_LINEAR_INTERPOLATION:
		return engine.IntrabarPolicyLinearInterpolation
	default:
		return engine.IntrabarPolicyExactTrades
	}
}

func (s *BacktestService) convertSlippageMode(mode pb.SlippageMode) engine.SlippageMode {
	switch mode {
	case pb.SlippageMode_NONE:
		return engine.SlippageModeNone
	case pb.SlippageMode_TRADE_SWEEP:
		return engine.SlippageModeTradeSweep
	case pb.SlippageMode_SYNTHETIC_BOOK:
		return engine.SlippageModeSyntheticBook
	default:
		return engine.SlippageModeNone
	}
}

func (s *BacktestService) convertToGrpcResponse(result *engine.BacktestResult) *pb.BacktestResponse {
	response := &pb.BacktestResponse{
		JobId:         result.JobID,
		ExecutionTime: int64(result.ExecutionTimeMs),
		SymbolResults: make([]*pb.SymbolResult, len(result.SymbolResults)),
		Manifest:      s.convertManifestToGrpc(result.Manifest),
	}

	for i, symbolResult := range result.SymbolResults {
		response.SymbolResults[i] = &pb.SymbolResult{
			Symbol:      symbolResult.Symbol,
			Trades:      s.convertTradesToGrpc(symbolResult.Trades),
			Positions:   s.convertPositionsToGrpc(symbolResult.Positions),
			EquityCurve: s.convertEquityCurveToGrpc(symbolResult.EquityCurve),
			Drawdown:    symbolResult.Drawdown.String(),
			Exposure:    symbolResult.Exposure.String(),
		}
	}

	return response
}

func (s *BacktestService) convertTradesToGrpc(trades []*engine.ExecutedTrade) []*pb.ExecutedTrade {
	grpcTrades := make([]*pb.ExecutedTrade, len(trades))
	for i, trade := range trades {
		grpcTrades[i] = &pb.ExecutedTrade{
			Timestamp:  int64(trade.Timestamp),
			Symbol:     trade.Symbol,
			Side:       s.convertTradeSideToGrpc(trade.Side),
			Quantity:   trade.Quantity.String(),
			Price:      trade.Price.String(),
			Fee:        trade.Fee.String(),
			Slippage:   trade.Slippage.String(),
			ReasonCode: trade.ReasonCode,
		}
	}
	return grpcTrades
}

func (s *BacktestService) convertPositionsToGrpc(positions []*engine.Position) []*pb.Position {
	grpcPositions := make([]*pb.Position, len(positions))
	for i, pos := range positions {
		grpcPositions[i] = &pb.Position{
			Timestamp:     int64(pos.Timestamp),
			Symbol:        pos.Symbol,
			Quantity:      pos.Quantity.String(),
			AvgPrice:      pos.AvgPrice.String(),
			UnrealizedPnl: pos.UnrealizedPnl.String(),
			RealizedPnl:   pos.RealizedPnl.String(),
		}
	}
	return grpcPositions
}

func (s *BacktestService) convertEquityCurveToGrpc(curve []*engine.EquityPoint) []*pb.EquityPoint {
	grpcCurve := make([]*pb.EquityPoint, len(curve))
	for i, point := range curve {
		grpcCurve[i] = &pb.EquityPoint{
			Timestamp: int64(point.Timestamp),
			Equity:    point.Equity.String(),
			Drawdown:  point.Drawdown.String(),
			Exposure:  point.Exposure.String(),
		}
	}
	return grpcCurve
}

func (s *BacktestService) convertTradeSideToGrpc(side engine.TradeSide) pb.TradeSide {
	switch side {
	case engine.TradeSideBuy:
		return pb.TradeSide_BUY
	case engine.TradeSideSell:
		return pb.TradeSide_SELL
	default:
		return pb.TradeSide_BUY
	}
}

func (s *BacktestService) convertManifestToGrpc(manifest *engine.RunManifest) *pb.RunManifest {
	return &pb.RunManifest{
		JobId:          manifest.JobID,
		SnapshotId:     manifest.SnapshotID,
		EngineVersion:  manifest.EngineVersion,
		StrategyHash:   manifest.StrategyHash,
		IntrabarPolicy: manifest.IntrabarPolicy,
		FeeVersion:     manifest.FeeVersion,
		SlippageMode:   manifest.SlippageMode,
		CreatedAt:      int64(manifest.CreatedAt),
		CpuFeatures:    manifest.CpuFeatures,
		FpFlags:        manifest.FpFlags,
	}
}

func (s *BacktestService) buildStrategyParameters(job *engine.BacktestJob) map[string]string {
	return map[string]string{
		"timeframe":       job.Timeframe,
		"intrabar_policy": string(job.IntrabarPolicy),
		"slippage_mode":   string(job.SlippageMode),
		"fee_version":     job.FeeVersion,
	}
}

func (s *BacktestService) createRunManifest(job *engine.BacktestJob) *engine.RunManifest {
	return &engine.RunManifest{
		JobID:          job.JobID,
		SnapshotID:     job.SnapshotID,
		EngineVersion:  "1.0.0", // Would get from actual engine
		StrategyHash:   job.StrategyWasmHash,
		IntrabarPolicy: string(job.IntrabarPolicy),
		FeeVersion:     job.FeeVersion,
		SlippageMode:   string(job.SlippageMode),
		CreatedAt:      uint64(time.Now().UnixMilli()),
		CpuFeatures:    []string{"simd", "avx2"}, // Would detect actual features
		FpFlags:        "nearest-even",
	}
}

// HTTP handlers for REST API
func (s *BacktestService) setupHTTPRoutes(r *gin.Engine) {
	api := r.Group("/api/v1")
	{
		api.POST("/backtest", s.handleBacktestRequest)
		api.GET("/backtest/:job_id", s.handleGetBacktestResult)
		api.GET("/health", s.handleHealthCheck)
		api.GET("/metrics", s.handleMetrics)
	}
}

func (s *BacktestService) handleBacktestRequest(c *gin.Context) {
	var req pb.BacktestRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Convert to gRPC request and execute
	ctx := c.Request.Context()
	resp, err := s.ExecuteBacktest(ctx, &req)
	if err != nil {
		s.logger.Error("Backtest request failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (s *BacktestService) handleGetBacktestResult(c *gin.Context) {
	jobID := c.Param("job_id")
	// Implementation would retrieve result from storage
	c.JSON(http.StatusOK, gin.H{"job_id": jobID, "status": "completed"})
}

func (s *BacktestService) handleHealthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
		"version":   "1.0.0",
	})
}

func (s *BacktestService) handleMetrics(c *gin.Context) {
	// Return Prometheus metrics
	c.Header("Content-Type", "text/plain")
	c.String(http.StatusOK, s.monitoring.GetMetrics())
}

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize logger
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer logger.Sync()

	logger.Info("Starting backtesting service",
		zap.String("version", "1.0.0"),
		zap.String("environment", cfg.Environment),
	)

	// Create backtest service
	service, err := NewBacktestService(cfg, logger)
	if err != nil {
		logger.Fatal("Failed to create backtest service", zap.Error(err))
	}

	// Setup gRPC server
	grpcServer := grpc.NewServer()
	pb.RegisterBacktestServiceServer(grpcServer, service)
	reflection.Register(grpcServer)

	// Setup HTTP server
	gin.SetMode(gin.ReleaseMode)
	httpRouter := gin.New()
	httpRouter.Use(gin.Recovery())
	service.setupHTTPRoutes(httpRouter)

	// Start servers
	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Server.GRPCPort))
		if err != nil {
			logger.Fatal("Failed to listen on gRPC port", zap.Error(err))
		}

		logger.Info("Starting gRPC server", zap.Int("port", cfg.Server.GRPCPort))
		if err := grpcServer.Serve(lis); err != nil {
			logger.Fatal("Failed to serve gRPC", zap.Error(err))
		}
	}()

	go func() {
		logger.Info("Starting HTTP server", zap.Int("port", cfg.Server.HTTPPort))
		if err := httpRouter.Run(fmt.Sprintf(":%d", cfg.Server.HTTPPort)); err != nil {
			logger.Fatal("Failed to serve HTTP", zap.Error(err))
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down servers...")
	grpcServer.GracefulStop()
	logger.Info("Servers stopped")
}
