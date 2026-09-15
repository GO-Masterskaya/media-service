package acceptance_test

import (
	"bytes"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	mediav1 "mediaservice/proto/media/v1"
)

func (s *AcceptanceSuite) TestHappyPath_ImageUploadDownloadDelete() {
	t := s.T()

	up := s.upload(t, uploadOpts{
		filename: "pixel.png",
		mime:     "image/png",
		body:     png64,
	})
	require.Equal(t, mediav1.MediaStatus_STORED, up.Status)

	m := s.waitStatus(t, up.MediaId, mediav1.MediaStatus_STORED)
	require.NotNil(t, m.Metadata)
	require.NotEmpty(t, m.Id)

	urlResp, err := s.client.GetDownloadURL(s.authCtx(), &mediav1.GetDownloadURLRequest{
		MediaId: up.MediaId,
		Variant: "original",
	})
	require.NoError(t, err)
	require.NotEmpty(t, urlResp.Url)
	got := httpGetBytes(t, urlResp.Url)
	require.True(t, bytes.Equal(png64, got), "presigned body mismatch")

	streamed := s.downloadStreamAll(t, up.MediaId, "original")
	require.True(t, bytes.Equal(png64, streamed), "stream body mismatch")

	_, err = s.client.DeleteMedia(s.authCtx(), &mediav1.DeleteMediaRequest{MediaId: up.MediaId})
	require.NoError(t, err)

	_, err = s.client.GetMedia(s.authCtx(), &mediav1.GetMediaRequest{MediaId: up.MediaId})
	requireGRPCCode(t, err, codes.NotFound)

	// Повторный delete после hard-delete — идемпотентный OK (ТЗ §7).
	_, err = s.client.DeleteMedia(s.authCtx(), &mediav1.DeleteMediaRequest{MediaId: up.MediaId})
	require.NoError(t, err)
}

func (s *AcceptanceSuite) TestHappyPath_ImageWithThumbnail() {
	t := s.T()

	up := s.upload(t, uploadOpts{
		filename:      "pixel.png",
		mime:          "image/png",
		body:          png64,
		makeThumbnail: true,
	})
	require.Equal(t, mediav1.MediaStatus_PROCESSING, up.Status)

	s.waitStatus(t, up.MediaId, mediav1.MediaStatus_READY)

	thumbURL, err := s.client.GetDownloadURL(s.authCtx(), &mediav1.GetDownloadURLRequest{
		MediaId: up.MediaId,
		Variant: "thumb",
	})
	require.NoError(t, err)
	require.NotEmpty(t, thumbURL.Url)
	require.NotEmpty(t, httpGetBytes(t, thumbURL.Url))
}

func (s *AcceptanceSuite) TestMimeForgery_Rejected() {
	t := s.T()

	stream, err := s.client.Upload(s.authCtx())
	require.NoError(t, err)
	require.NoError(t, stream.Send(&mediav1.UploadRequest{
		Payload: &mediav1.UploadRequest_Init{Init: &mediav1.UploadInit{
			OwnerId:        s.ownerID.String(),
			Filename:       "evil.png",
			Mime:           "image/png",
			ExpectedSize:   uint64(len(exeBody)),
			IdempotencyKey: uuid.NewString(),
		}},
	}))
	require.NoError(t, stream.Send(&mediav1.UploadRequest{
		Payload: &mediav1.UploadRequest_Chunk{Chunk: exeBody},
	}))
	_, err = stream.CloseAndRecv()
	requireGRPCCode(t, err, codes.InvalidArgument)
}

func (s *AcceptanceSuite) TestIdempotency_SameKeyNoDuplicate() {
	t := s.T()
	key := uuid.NewString()

	first := s.upload(t, uploadOpts{
		filename:       "pixel.png",
		mime:           "image/png",
		body:           png64,
		idempotencyKey: key,
	})
	second := s.upload(t, uploadOpts{
		filename:       "pixel.png",
		mime:           "image/png",
		body:           png64,
		idempotencyKey: key,
	})
	require.Equal(t, first.MediaId, second.MediaId)

	list, err := s.client.ListMediaByOwner(s.authCtx(), &mediav1.ListMediaByOwnerRequest{
		OwnerId:  s.ownerID.String(),
		PageSize: 100,
	})
	require.NoError(t, err)
	count := 0
	for _, item := range list.Items {
		if item.Id == first.MediaId {
			count++
		}
	}
	require.Equal(t, 1, count, "idempotent upload must not create a duplicate row")
}

func (s *AcceptanceSuite) TestListMediaByOwner_Paginates() {
	t := s.T()
	var ids []string
	for i := 0; i < 3; i++ {
		up := s.upload(t, uploadOpts{
			filename: "pixel.png",
			mime:     "image/png",
			body:     png64,
		})
		ids = append(ids, up.MediaId)
	}

	page1, err := s.client.ListMediaByOwner(s.authCtx(), &mediav1.ListMediaByOwnerRequest{
		OwnerId:  s.ownerID.String(),
		PageSize: 2,
	})
	require.NoError(t, err)
	require.Len(t, page1.Items, 2)
	require.NotEmpty(t, page1.NextPageToken)

	page2, err := s.client.ListMediaByOwner(s.authCtx(), &mediav1.ListMediaByOwnerRequest{
		OwnerId:   s.ownerID.String(),
		PageSize:  2,
		PageToken: page1.NextPageToken,
	})
	require.NoError(t, err)
	require.NotEmpty(t, page2.Items)

	seen := map[string]struct{}{}
	for _, it := range append(page1.Items, page2.Items...) {
		seen[it.Id] = struct{}{}
	}
	for _, id := range ids {
		_, ok := seen[id]
		require.True(t, ok, "uploaded media %s missing from list pages", id)
	}
}

