package acceptance_test

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	mediav1 "mediaservice/proto/media/v1"
)

func (s *AcceptanceSuite) TestKafka_DetachDeletesMedia() {
	t := s.T()

	up := s.upload(t, uploadOpts{
		filename: "pixel.png",
		mime:     "image/png",
		body:     png64,
	})
	s.waitStatus(t, up.MediaId, mediav1.MediaStatus_STORED)

	mediaID := uuid.MustParse(up.MediaId)
	eventID := uuid.New()
	s.produceDetach(t, eventID, mediaID, s.ownerID)
	s.waitMediaGone(t, up.MediaId)
}

func (s *AcceptanceSuite) TestKafka_DuplicateEventID_NoBreak() {
	t := s.T()

	up := s.upload(t, uploadOpts{
		filename: "pixel.png",
		mime:     "image/png",
		body:     png64,
	})
	s.waitStatus(t, up.MediaId, mediav1.MediaStatus_STORED)

	mediaID := uuid.MustParse(up.MediaId)
	eventID := uuid.New()
	raw := s.produceDetach(t, eventID, mediaID, s.ownerID)
	s.waitMediaGone(t, up.MediaId)

	// Повтор того же event_id — already processed, сервис не падает.
	s.produceKafka(t, kafkaTopic, []byte(eventID.String()), raw)

	// Контроль: новый upload всё ещё работает.
	up2 := s.upload(t, uploadOpts{
		filename: "pixel2.png",
		mime:     "image/png",
		body:     png64,
	})
	require.NotEmpty(t, up2.MediaId)
	s.waitStatus(t, up2.MediaId, mediav1.MediaStatus_STORED)
}

func (s *AcceptanceSuite) TestKafka_InvalidEnvelope_GoesToDLQ() {
	t := s.T()

	poison := []byte(`{"not":"a valid envelope"`)
	s.produceKafka(t, kafkaTopic, []byte("poison"), poison)
	s.waitDLQContains(t, string(poison))

	// Поток жив: обычный upload после poison.
	up := s.upload(t, uploadOpts{
		filename: "after-poison.png",
		mime:     "image/png",
		body:     png64,
	})
	require.NotEmpty(t, up.MediaId)
}

func (s *AcceptanceSuite) TestKafkaEnabled_GRPCStillWorks() {
	t := s.T()
	up := s.upload(t, uploadOpts{body: png64})
	require.NotEmpty(t, up.MediaId)
	_, err := s.client.GetMedia(s.authCtx(), &mediav1.GetMediaRequest{MediaId: up.MediaId})
	require.NoError(t, err)
}
