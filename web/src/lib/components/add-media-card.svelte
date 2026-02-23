<script lang="ts">
	import { Button } from '$lib/components/ui/button/index.js';
	import * as Card from '$lib/components/ui/card/index.js';
	import { ImageOff, LoaderCircle } from 'lucide-svelte';
	import { goto } from '$app/navigation';
	import { resolve } from '$app/paths';
	import type { components } from '$lib/api/api';
	import client from '$lib/api';

	let loading = $state(false);
	let errorMessage = $state<string | null>(null);
	let {
		result,
		isShow = true
	}: { result: components['schemas']['MetaDataProviderSearchResult']; isShow: boolean } = $props();

	async function addMedia() {
		loading = true;
		let data;
		if (isShow) {
			const response = await client.POST('/api/v1/tv/shows', {
				params: {
					query: {
						show_id: result.external_id,
						metadata_provider: result.metadata_provider as 'tmdb' | 'tvdb',
						language: result.original_language ?? undefined
					}
				}
			});
			data = response.data;
		} else {
			const response = await client.POST('/api/v1/movies', {
				params: {
					query: {
						movie_id: result.external_id,
						metadata_provider: result.metadata_provider as 'tmdb' | 'tvdb',
						language: result.original_language ?? undefined
					}
				}
			});
			data = response.data;
		}

		if (isShow) {
			await goto(resolve('/dashboard/tv/[showId]', { showId: data?.id ?? '' }), {
				invalidateAll: true
			});
		} else {
			await goto(resolve('/dashboard/movies/[movieId]', { movieId: data?.id ?? '' }), {
				invalidateAll: true
			});
		}
		loading = false;
	}
</script>

<Card.Root class="flex h-[350px] w-full flex-col overflow-hidden">
	<Card.Header class="p-4 pb-2">
		<Card.Title class="truncate text-base leading-tight">
			{result.name}
			{#if result.year != null}
				<span class="text-sm font-normal text-muted-foreground">({result.year})</span>
			{/if}
		</Card.Title>
		<Card.Description class="line-clamp-2 text-xs"
			>{result.overview !== '' ? result.overview : 'No overview available'}</Card.Description
		>
	</Card.Header>
	<Card.Content class="flex flex-1 items-center justify-center overflow-hidden p-0 px-4 pb-2">
		{#if result.poster_path != null}
			<img
				class="h-full w-full rounded-md object-cover"
				src={result.poster_path}
				alt="{result.name}'s Poster Image"
			/>
		{:else}
			<div class="flex h-full w-full items-center justify-center rounded-md bg-muted/20">
				<ImageOff class="h-8 w-8 text-muted-foreground/50" />
			</div>
		{/if}
	</Card.Content>
	<Card.Footer class="flex flex-col items-start gap-2 rounded-b-lg border-t bg-card p-4">
		{#if result.added}
			<Button
				class="w-full font-semibold"
				variant="secondary"
				href={resolve(
					isShow ? '/dashboard/tv/[showId]' : '/dashboard/movies/[movieId]',
					isShow ? { showId: result.id ?? '' } : { movieId: result.id ?? '' }
				)}
			>
				{isShow ? 'Show already exists' : 'Movie already exists'}
			</Button>
		{:else}
			<Button class="w-full font-semibold" disabled={loading} onclick={() => addMedia()}>
				{#if loading}
					<LoaderCircle class="mr-2 h-4 w-4 animate-spin" />
					<span class="animate-pulse">Loading...</span>
				{:else}
					{`Add ${isShow ? 'Show' : 'Movie'}`}
				{/if}
			</Button>
		{/if}
		<div class="flex w-full items-center gap-2">
			{#if result.vote_average != null}
				<span class="flex items-center text-sm font-medium text-yellow-600">
					<svg class="mr-1 h-4 w-4 text-yellow-400" fill="currentColor" viewBox="0 0 20 20"
						><path
							d="M10 15l-5.878 3.09 1.122-6.545L.488 6.91l6.561-.955L10 0l2.951 5.955 6.561.955-4.756 4.635 1.122 6.545z"
						/></svg
					>
					Rating: {Math.round(result.vote_average)}/10
				</span>
			{/if}
		</div>
		{#if errorMessage}
			<p class="w-full rounded bg-red-50 px-2 py-1 text-xs text-red-500">{errorMessage}</p>
		{/if}
	</Card.Footer>
</Card.Root>
