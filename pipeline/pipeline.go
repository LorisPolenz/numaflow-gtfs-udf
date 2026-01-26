package pipeline

import "numaflow_gtfs_udf/transformer"

// do atomic thing on data
type stage struct {
	name        string
	transformer transformer.Transformer
}

// compine multiple stages
type pipeline struct {
	name   string
	stages []stage
}

func NewPipeline(name string) *pipeline {
	return &pipeline{
		name:   name,
		stages: []stage{},
	}
}

// pipeline is the receiver, so we can add stages to it (pipeline is a method of the pipeline type)
func (p *pipeline) AddStage(name string, t transformer.Transformer) *pipeline {
	p.stages = append(p.stages, stage{name: name, transformer: t})
	return p
}

func (p *pipeline) Run() {
	for _, s := range p.stages {
		s.transformer.Transform()
	}
}
