package mediaservice_test

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"mediaservice/pkg/mediaservice"
)

// TestListByOwner_WalksAllPages - полный обход выборки страницами
// не теряет и не повторяет записи.
//
// Обход именно циклом, а не двумя запросами: ошибка в границе страницы
// проявляется на стыке, и с двумя запросами её видно только если повезёт
// с числом записей. Семь записей по три страницы дают неполную последнюю -
// самый интересный случай.
func (s *ClientSuite) TestListByOwner_WalksAllPages() {
	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	owner := uuid.New()
	const total = 7

	want := make(map[uuid.UUID]struct{}, total)
	for i := 0; i < total; i++ {
		want[s.insertMedia(owner, "stored")] = struct{}{}
	}

	got := make(map[uuid.UUID]struct{}, total)
	token := ""
	pages := 0

	for {
		res, err := client.ListByOwner(s.ctx, mediaservice.ListParams{
			OwnerID:   owner,
			PageSize:  3,
			PageToken: token,
		})
		require.NoError(t, err)

		for _, m := range res.Items {
			_, dup := got[m.ID]
			require.False(t, dup, "запись %s встретилась дважды", m.ID)
			got[m.ID] = struct{}{}
		}

		pages++
		require.LessOrEqual(t, pages, total, "обход не сходится, вероятно курсор не двигается")

		token = res.NextPageToken
		if token == "" {
			break
		}
	}

	require.Equal(t, want, got)
	require.Equal(t, 3, pages, "семь записей по три - это три страницы")
}

// TestListByOwner_DefaultPageSize - нулевой PageSize означает умолчание,
// а не пустую выборку.
func (s *ClientSuite) TestListByOwner_DefaultPageSize() {
	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	owner := uuid.New()
	s.insertMedia(owner, "stored")
	s.insertMedia(owner, "stored")

	res, err := client.ListByOwner(s.ctx, mediaservice.ListParams{OwnerID: owner})
	require.NoError(t, err)
	require.Len(t, res.Items, 2)
	require.Empty(t, res.NextPageToken)
}

// TestListByOwner_PageSizeAboveCap - запрос сверх потолка отвергается,
// а не срезается молча.
//
// Тихое срезание оставило бы вызывающего в уверенности, что он увидел всё:
// попросил пять тысяч, получил тысячу и пустой токен - значит объектов
// ровно тысяча. Явная ошибка такого вывода не допускает.
func (s *ClientSuite) TestListByOwner_PageSizeAboveCap() {
	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	_, err := client.ListByOwner(s.ctx, mediaservice.ListParams{
		OwnerID:  uuid.New(),
		PageSize: mediaservice.MaxPageSize + 1,
	})
	require.ErrorIs(t, err, mediaservice.ErrInvalidArgument)
}

// TestListByOwner_TokenBoundToOwner - токен одного владельца не продолжает
// выборку другого.
//
// Утечки данных без этой проверки не было бы - выборка всё равно идёт
// по своему owner_id. Но результат оказался бы молча сдвинут на позицию
// из чужой ленты, и вызывающий получил бы не ту страницу, ничего
// не заметив.
func (s *ClientSuite) TestListByOwner_TokenBoundToOwner() {
	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	alice := uuid.New()
	bob := uuid.New()
	for i := 0; i < 3; i++ {
		s.insertMedia(alice, "stored")
		s.insertMedia(bob, "stored")
	}

	page, err := client.ListByOwner(s.ctx, mediaservice.ListParams{
		OwnerID:  alice,
		PageSize: 1,
	})
	require.NoError(t, err)
	require.NotEmpty(t, page.NextPageToken)

	_, err = client.ListByOwner(s.ctx, mediaservice.ListParams{
		OwnerID:   bob,
		PageSize:  1,
		PageToken: page.NextPageToken,
	})
	require.ErrorIs(t, err, mediaservice.ErrInvalidArgument)
}

// TestListByOwner_MalformedToken - мусор вместо токена это ошибка
// аргумента, а не паника и не ErrInternal.
//
// Токен приходит снаружи и может быть любым: обрезанным при копировании,
// подобранным, оставшимся от прошлой версии формата. Разбор обязан
// пережить всё это.
func (s *ClientSuite) TestListByOwner_MalformedToken() {
	client := s.newClient()
	defer func() { _ = client.Close() }()

	owner := uuid.New()

	tokens := map[string]string{
		"не base64":        "not-a-token!!",
		"base64 без json":  "YWJjZGVm",
		"пустой json":      "e30",
		"обрезанный токен": "eyJ2IjoxLCJvd25l",
	}

	for name, token := range tokens {
		s.Run(name, func() {
			_, err := client.ListByOwner(s.ctx, mediaservice.ListParams{
				OwnerID:   owner,
				PageToken: token,
			})
			require.ErrorIs(s.T(), err, mediaservice.ErrInvalidArgument)
		})
	}
}

// TestListByOwner_RejectsNilOwner - выборка без владельца бессмысленна.
func (s *ClientSuite) TestListByOwner_RejectsNilOwner() {
	t := s.T()
	client := s.newClient()
	defer func() { _ = client.Close() }()

	_, err := client.ListByOwner(s.ctx, mediaservice.ListParams{})
	require.ErrorIs(t, err, mediaservice.ErrInvalidArgument)
}
