<script lang="ts">
	import AddMediaCard from '$lib/components/add-media-card.svelte';
	import { Skeleton } from '$lib/components/ui/skeleton';
	import { Button } from '$lib/components/ui/button';
	import { ChevronRight } from 'lucide-svelte';
	import type { components } from '$lib/api/api';
	import { resolve } from '$app/paths';

	let {
		media,
		isShow,
		isLoading
	}: {
		media: components['schemas']['MetaDataProviderSearchResult'][];
		isShow: boolean;
		isLoading: boolean;
	} = $props();
</script>

<div
	class="grid w-full grid-cols-1 gap-4 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5"
>
	{#if isLoading}
		<Skeleton class="h-[350px] w-full" />
		<Skeleton class="hidden h-[350px] w-full sm:block" />
		<Skeleton class="hidden h-[350px] w-full md:block" />
		<Skeleton class="hidden h-[350px] w-full lg:block" />
		<Skeleton class="hidden h-[350px] w-full xl:block" />
	{:else}
		{#each media.slice(0, 5) as mediaItem (mediaItem.external_id)}
			<AddMediaCard {isShow} result={mediaItem} />
		{/each}
	{/if}
	{#if isShow}
		<Button class="md:col-start-2" variant="secondary" href={resolve('/dashboard/tv/add-show', {})}>
			More recommendations
			<ChevronRight />
		</Button>
	{:else}
		<Button
			class="md:col-start-2"
			variant="secondary"
			href={resolve('/dashboard/movies/add-movie', {})}
		>
			More recommendations
			<ChevronRight />
		</Button>
	{/if}
</div>
