package integration

import (
	"bytes"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"
)

type testConfigDetails struct {
	// A unique identifier for separating this test configuration from others.
	// Usually used to access a different topic, consumer group, directory, etc.
	id string

	// Used by batching testers to check the input honours batching fields.
	inputBatchCount int

	// Used by batching testers to check the output honours batching fields.
	outputBatchCount int
}

type testConfigCtor func(t *testing.T, details testConfigDetails) string

func newTestConfigDetails(t *testing.T) testConfigDetails {
	t.Helper()

	u4, err := uuid.NewV4()
	require.NoError(t, err)

	return testConfigDetails{
		id: u4.String(),
	}
}

type testEnvironment struct {
	config           string
	input            types.Input
	output           types.Output
	connectorContext connectorContext
}

func newTestEnvironment(conf string) *testEnvironment {
	return &testEnvironment{
		config: conf,
		connectorContext: connectorContext{
			log:   log.Noop(),
			stats: metrics.Noop(),
		},
	}
}

type testDefinition interface {
	SetDetails(*testing.T, *testConfigDetails)
	Execute(*testing.T, *testEnvironment)
}

func runIntegrationTests(
	t *testing.T,
	configCtor testConfigCtor,
	tests ...testDefinition,
) {
	for _, test := range tests {
		details := newTestConfigDetails(t)
		test.SetDetails(t, &details)
		test.Execute(t, newTestEnvironment(configCtor(t, details)))
	}
}

//------------------------------------------------------------------------------

type testDefinitionFn struct {
	setDetails func(*testing.T, *testConfigDetails)
	execute    func(*testing.T, *testEnvironment)
}

func (def testDefinitionFn) SetDetails(t *testing.T, d *testConfigDetails) {
	if def.setDetails != nil {
		def.setDetails(t, d)
	}
}

func (def testDefinitionFn) Execute(t *testing.T, env *testEnvironment) {
	def.execute(t, env)
}

func newTest(
	name string,
	detailsFn func(*testing.T, *testConfigDetails),
	executeFn func(*testing.T, *testEnvironment),
) testDefinition {
	return testDefinitionFn{
		setDetails: detailsFn,
		execute: func(t *testing.T, env *testEnvironment) {
			t.Run(name, func(t *testing.T) {
				executeFn(t, env)
			})
		},
	}
}

//------------------------------------------------------------------------------

type connectorContext struct {
	log   log.Modular
	stats metrics.Type
}

func initConnectors(t *testing.T, env *testEnvironment) (types.Input, types.Output) {
	t.Helper()

	s := config.New()
	dec := yaml.NewDecoder(bytes.NewReader([]byte(env.config)))
	dec.KnownFields(true)
	require.NoError(t, dec.Decode(&s))

	lints, err := config.Lint([]byte(env.config), s)
	require.NoError(t, err)
	assert.Empty(t, lints)

	input, err := input.New(s.Input, types.NoopMgr(), env.connectorContext.log, env.connectorContext.stats)
	require.NoError(t, err)

	output, err := output.New(s.Output, types.NoopMgr(), env.connectorContext.log, env.connectorContext.stats)
	require.NoError(t, err)

	return input, output
}

func closeConnectors(t *testing.T, input types.Input, output types.Output) {
	output.CloseAsync()
	require.NoError(t, output.WaitForClose(time.Second*10))

	input.CloseAsync()
	require.NoError(t, input.WaitForClose(time.Second*10))
}

//------------------------------------------------------------------------------

var integrationTestOpenClose = newTest("check_open_close", nil, func(t *testing.T, env *testEnvironment) {
	input, output := initConnectors(t, env)
	closeConnectors(t, input, output)
})
