package manager

import (
	"context"
	"sync"

	"github.com/khulnasoft/kagent/manager/config"
	"github.com/khulnasoft/kagent/pipeline"
	"github.com/khulnasoft/kagent/pipeline/build"
	"github.com/khulnasoft/kagent/pipeline/discovery"
	"github.com/khulnasoft/kagent/pipeline/export"
	"github.com/khulnasoft/kagent/pipeline/tag"
	"github.com/khulnasoft/kagent/pkg/log"

	"github.com/rs/zerolog"
)

type (
	Manager struct {
		prov      ConfigProvider
		factory   factory
		cache     map[string]uint64
		pipelines map[string]func()
		log       zerolog.Logger
	}
	ConfigProvider interface {
		Run(ctx context.Context)
		Configs() chan []config.Config
	}
	kagentPipeline interface {
		Run(ctx context.Context)
	}
	factory interface {
		create(cfg config.PipelineConfig) (kagentPipeline, error)
	}
	factoryFunc func(cfg config.PipelineConfig) (kagentPipeline, error)
)

func (f factoryFunc) create(cfg config.PipelineConfig) (kagentPipeline, error) { return f(cfg) }

func New(provider ConfigProvider) *Manager {
	return &Manager{
		prov:      provider,
		factory:   factoryFunc(newPipeline),
		cache:     make(map[string]uint64),
		pipelines: make(map[string]func()),
		log:       log.New("pipeline manager"),
	}
}

func (m *Manager) Run(ctx context.Context) {
	m.log.Info().Msg("instance is started")
	defer m.log.Info().Msg("instance is stopped")
	defer m.cleanup()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() { defer wg.Done(); m.prov.Run(ctx) }()

	wg.Add(1)
	go func() { defer wg.Done(); m.run(ctx) }()

	wg.Wait()
	<-ctx.Done()
}

func (m *Manager) cleanup() {
	for _, stop := range m.pipelines {
		stop()
	}
}

func (m *Manager) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case cfgs := <-m.prov.Configs():
			for _, cfg := range cfgs {
				select {
				case <-ctx.Done():
					return
				default:
					m.process(ctx, cfg)
				}
			}
		}
	}
}

func (m *Manager) process(ctx context.Context, cfg config.Config) {
	if cfg.Source == "" {
		return
	}

	if cfg.Pipeline == nil {
		delete(m.cache, cfg.Source)
		m.handleRemoveConfig(cfg)
		return
	}

	if hash, ok := m.cache[cfg.Source]; !ok || hash != cfg.Pipeline.Hash() {
		m.cache[cfg.Source] = cfg.Pipeline.Hash()
		m.handleNewConfig(ctx, cfg)
	}
}

func (m *Manager) handleRemoveConfig(cfg config.Config) {
	if stop, ok := m.pipelines[cfg.Source]; ok {
		m.log.Info().Msgf("received an empty config, stopping the pipeline ('%s')", cfg.Source)
		delete(m.pipelines, cfg.Source)
		stop()
	}
}

func (m *Manager) handleNewConfig(ctx context.Context, cfg config.Config) {
	p, err := m.factory.create(*cfg.Pipeline)
	if err != nil {
		if _, ok := m.pipelines[cfg.Source]; ok {
			m.log.Warn().Err(err).Msgf("unable to create a pipeline, will keep using old config ('%s')",
				cfg.Source)
		} else {
			m.log.Warn().Err(err).Msgf("unable to create a pipeline ('%s')", cfg.Source)
		}
		return
	}

	if stop, ok := m.pipelines[cfg.Source]; ok {
		m.log.Info().Msgf("received an updated config, restarting the pipeline ('%s')", cfg.Source)
		stop()
	} else {
		m.log.Info().Msgf("received a new config, starting a new pipeline ('%s')", cfg.Source)
	}

	var wg sync.WaitGroup
	pipelineCtx, cancel := context.WithCancel(ctx)

	wg.Add(1)
	go func() { defer wg.Done(); p.Run(pipelineCtx) }()
	stop := func() { cancel(); wg.Wait() }

	m.pipelines[cfg.Source] = stop
}

func newPipeline(cfg config.PipelineConfig) (kagentPipeline, error) {
	exporter, err := export.New(cfg.Export)
	if err != nil {
		return nil, err
	}
	builder, err := build.New(cfg.Build)
	if err != nil {
		return nil, err
	}
	tagger, err := tag.New(cfg.Tag)
	if err != nil {
		return nil, err
	}
	discoverer, err := discovery.New(cfg.Discovery)
	if err != nil {
		return nil, err
	}
	return pipeline.New(discoverer, tagger, builder, exporter), nil
}
