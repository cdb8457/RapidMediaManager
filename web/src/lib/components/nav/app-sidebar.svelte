<script lang="ts" module>
	import { Bell, Database, Home, Info, LifeBuoy, Settings, ListVideo } from 'lucide-svelte';
	import { resolve } from '$app/paths';

	import { PUBLIC_VERSION } from '$env/static/public';

	const data = {
		navMain: [
			{
				title: 'Dashboard',
				url: resolve('/dashboard', {}),
				icon: Home,
				isActive: true
			}
		],
		navSecondary: [
			{
				title: 'Seerr Requests',
				url: resolve('/dashboard/requests', {}),
				icon: ListVideo
			},
			{
				title: 'Debrid Cache',
				url: resolve('/dashboard/transfers', {}),
				icon: Database
			},
			{
				title: 'Notifications',
				url: resolve('/dashboard/notifications', {}),
				icon: Bell
			},
			{
				title: 'Documentation',
				url: 'https://github.com/your-org/RapidMediaManager',
				icon: LifeBuoy
			},
			{
				title: 'Settings',
				url: resolve('/dashboard/settings', {}),
				icon: Settings
			},
			{
				title: 'About',
				url: resolve('/dashboard/about', {}),
				icon: Info
			}
		]
	};
</script>

<script lang="ts">
	import NavMain from '$lib/components/nav/nav-main.svelte';
	import NavSecondary from '$lib/components/nav/nav-secondary.svelte';
	import NavUser from '$lib/components/nav/nav-user.svelte';
	import * as Sidebar from '$lib/components/ui/sidebar';
	import type { ComponentProps } from 'svelte';
	import { base } from '$app/paths';

	let { ref = $bindable(null), ...restProps }: ComponentProps<typeof Sidebar.Root> = $props();
</script>

<Sidebar.Root {...restProps} bind:ref variant="inset">
	<Sidebar.Header>
		<Sidebar.Menu>
			<Sidebar.MenuItem>
				<Sidebar.MenuButton size="lg">
					{#snippet child({ props })}
						<a href={resolve('/dashboard', {})} {...props} class="flex items-center gap-2">
							<img
								class="size-10 rounded-md object-cover"
								src="{base}/rapid_engine_logo.png"
								alt="Rapid Engine Logo"
							/>
							<div class="grid flex-1 text-left text-sm leading-tight">
								<span class="truncate font-semibold">Rapid Engine</span>
								<span class="truncate text-xs">Admin Control Panel</span>
							</div>
						</a>
					{/snippet}
				</Sidebar.MenuButton>
			</Sidebar.MenuItem>
		</Sidebar.Menu>
	</Sidebar.Header>
	<Sidebar.Content>
		<NavMain items={data.navMain} />
		<!--  <NavProjects projects={data.projects}/> -->
		<NavSecondary class="mt-auto" items={data.navSecondary} />
	</Sidebar.Content>
	<Sidebar.Footer>
		<NavUser />
	</Sidebar.Footer>
</Sidebar.Root>
